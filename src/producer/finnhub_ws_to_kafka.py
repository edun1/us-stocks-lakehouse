import asyncio
import json
import os
import time
from typing import Any, Dict, List

import websockets
from confluent_kafka import Producer
from dotenv import load_dotenv


def kafka_producer() -> Producer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": "finnhub-ws-producer",
        "enable.idempotence": True,
        "acks": "all",
        "retries": 10,
        "linger.ms": 50,
        "compression.type": "lz4",
    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err is not None:
        print(f"[kafka] delivery failed: {err}")


def finnhub_event_to_records(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Finnhub trade payload example:
    {"data":[{"p":171.25,"s":"AAPL","t":1710000000000,"v":100}, ...], "type":"trade"}
    We'll output one record per trade with a normalized envelope.
    """
    if payload.get("type") != "trade":
        return []
    out = []
    for d in payload.get("data", []) or []:
        out.append({
            "T": "t",              # trade type tag (Alpaca-like)
            "S": d.get("s"),       # symbol
            "p": d.get("p"),       # price
            "s": d.get("v"),       # size/volume (note: finnhub uses v)
            "t": d.get("t"),       # epoch ms
            "_src": "finnhub",
        })
    return out


def event_key(evt: Dict[str, Any]) -> str:
    sym = evt.get("S", "UNKNOWN")
    ts = evt.get("t", "")
    px = evt.get("p", "")
    sz = evt.get("s", "")
    return f"{sym}|{ts}|{px}|{sz}"


async def run():
    load_dotenv(dotenv_path=".env")

    api_key = os.getenv("FINNHUB_API_KEY")
    ws_url = os.getenv("FINNHUB_WS_URL", "wss://ws.finnhub.io")
    symbols = [s.strip().upper() for s in os.getenv("FINNHUB_SYMBOLS", "AAPL").split(",") if s.strip()]

    if not api_key:
        raise SystemExit("Missing FINNHUB_API_KEY in .env")

    p = kafka_producer()

    backoff = 1
    while True:
        try:
            url = f"{ws_url}?token={api_key}"
            print(f"[ws] connecting: {url}")

            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                print("[ws] connected")

                # subscribe to symbols
                for sym in symbols:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))
                print(f"[ws] subscribed: {symbols}")

                backoff = 1
                last_flush = time.time()

                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                    except Exception as e:
                        p.produce("dead_letter", value=json.dumps({"error": str(e), "raw": raw}).encode("utf-8"),
                                  on_delivery=delivery_callback)
                        continue

                    # Convert payload to trade records
                    records = finnhub_event_to_records(payload)
                    for evt in records:
                        evt["_ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        key = event_key(evt)
                        p.produce("trades", key=key.encode("utf-8"),
                                  value=json.dumps(evt).encode("utf-8"),
                                  on_delivery=delivery_callback)

                    if time.time() - last_flush > 1.0:
                        p.poll(0)
                        p.flush(0.5)
                        last_flush = time.time()

        except Exception as e:
            print(f"[ws] disconnected: {repr(e)}")
            print(f"[ws] reconnecting in {backoff}s...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\n[main] stopped")

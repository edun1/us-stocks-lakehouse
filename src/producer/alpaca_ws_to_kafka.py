import asyncio
import json
import os
import time
from typing import Any, Dict, List

from confluent_kafka import Producer
from dotenv import load_dotenv
import websockets


def kafka_producer() -> Producer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    conf = {
        "bootstrap.servers": bootstrap,
        "client.id": "alpaca-ws-producer",
        # dev-friendly reliability
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
    # else: you can print success, but it gets noisy fast


def event_to_topic(evt: Dict[str, Any]) -> str:
    """
    Alpaca market data stream events typically have a 'T' field indicating type.
    We'll route trades to 'trades', quotes to 'quotes', everything else to 'dead_letter'.
    """
    t = evt.get("T")
    if t == "t":   # trade
        return "trades"
    if t == "q":   # quote
        return "quotes"
    if t in ("success", "error", "subscription"):
        return "dead_letter"
    return "dead_letter"


def event_key(evt: Dict[str, Any]) -> str:
    """
    A simple deterministic key so Kafka partitioning is stable.
    For real dedupe later we’ll build a stronger event_id in Silver.
    """
    sym = evt.get("S", "UNKNOWN")
    ts = evt.get("t", evt.get("timestamp", ""))
    px = evt.get("p", "")
    sz = evt.get("s", "")
    exch = evt.get("x", "")
    return f"{sym}|{ts}|{exch}|{px}|{sz}"


async def run():
    load_dotenv()

    key_id = os.getenv("ALPACA_KEY_ID")
    secret = os.getenv("ALPACA_SECRET_KEY")
    ws_url = os.getenv("ALPACA_WS_URL", "wss://stream.data.alpaca.markets/v2/test")
    symbols = [s.strip() for s in os.getenv("ALPACA_SYMBOLS", "FAKEPACA").split(",") if s.strip()]

    if not key_id or not secret:
        raise SystemExit("Missing ALPACA_KEY_ID / ALPACA_SECRET_KEY in .env")

    p = kafka_producer()

    backoff = 1
    while True:
        try:
            print(f"[ws] connecting: {ws_url}")
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                # 1) auth
                #await ws.send(json.dumps({"action": "auth", "key": key_id, "secret": secret}))
                #print("[ws] sent auth")

                # read & print server response (usually success or error)
                first = await ws.recv()
                print(f"[ws] server: {first}")

                # 2) subscribe to test trades (FAKEPACA works on test stream) :contentReference[oaicite:2]{index=2}
                await ws.send(json.dumps({"action": "subscribe", "trades": symbols}))
                print(f"[ws] subscribed to trades: {symbols}")

                second = await ws.recv()
                print(f"[ws] server: {second}")

                # reset backoff after successful connect
                backoff = 1

                last_flush = time.time()

                # main loop (this exits cleanly if server closes)
                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                    except Exception as e:
                        evt = {"T": "dead", "error": str(e), "raw": raw}
                        p.produce("dead_letter", value=json.dumps(evt).encode("utf-8"), on_delivery=delivery_callback)
                        continue

                    events = payload if isinstance(payload, list) else [payload]
                    for evt in events:
                        topic = event_to_topic(evt)
                        key = event_key(evt)
                        evt["_ingested_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                        evt["_source"] = "alpaca_ws"
                        p.produce(
                            topic,
                            key=key.encode("utf-8"),
                            value=json.dumps(evt).encode("utf-8"),
                            on_delivery=delivery_callback,
                        )

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

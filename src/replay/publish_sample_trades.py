"""Publish the synthetic sample file to Kafka."""

import json
import os
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv


def main():
    load_dotenv()

    root = Path(__file__).resolve().parents[2]
    source = (
        root
        / "data/bronze/trades/dt=2026-01-05/hour=14"
        / "part-synthetic-replay.jsonl"
    )

    if not source.exists():
        raise SystemExit("Run generate_sample_trades.py first.")

    producer = Producer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 30000,
    })

    delivered = 0
    failures = []

    def on_delivery(error, message):
        nonlocal delivered
        if error:
            failures.append(str(error))
        else:
            delivered += 1

    queued = 0

    with source.open(encoding="utf-8") as file:
        for line in file:
            record = json.loads(line)

            while True:
                try:
                    producer.produce(
                        "trades_replay",
                        key=record["S"].encode("utf-8"),
                        value=json.dumps(record).encode("utf-8"),
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:
                    producer.poll(0.1)

            queued += 1
            producer.poll(0)

    remaining = producer.flush(40)

    if remaining or failures or delivered != queued:
        raise RuntimeError(
            f"Delivery incomplete: queued={queued}, "
            f"delivered={delivered}, pending={remaining}, "
            f"errors={failures[:3]}"
        )

    print(f"Published {delivered:,} synthetic trades to trades_replay")


if __name__ == "__main__":
    main()
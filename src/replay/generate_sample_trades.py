"""Generate reproducible synthetic trades. No live market data or API keys."""

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main():
    rng = random.Random(42)
    start = datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
    prices = {"AAPL": 20000, "MSFT": 40000, "NVDA": 15000}
    records = []

    for minute in range(30):
        for symbol in prices:
            for second in range(20):
                timestamp = start + timedelta(minutes=minute, seconds=second)
                prices[symbol] += rng.randint(-5, 5)

                records.append({
                    "T": "t",
                    "S": symbol,
                    "p": prices[symbol] / 100,
                    "s": rng.randint(1, 100),
                    "t": int(timestamp.timestamp() * 1000),
                    "_src": "synthetic_replay",
                    "_ingested_at": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                })

    project_root = Path(__file__).resolve().parents[2]
    output = (
        project_root
        / "data/bronze/trades/dt=2026-01-05/hour=14"
        / "part-synthetic-replay.jsonl"
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")

    print(f"Generated {len(records):,} synthetic trades")
    print(f"Saved to: {output}")
    print("Rerunning replaces this sample file with identical records.")


if __name__ == "__main__":
    main()
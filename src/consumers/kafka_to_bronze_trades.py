import os
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import orjson
from confluent_kafka import Consumer
from dotenv import load_dotenv


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_hour_path(base: Path, topic: str, ts: datetime) -> Path:
    dt = ts.strftime("%Y-%m-%d")
    hour = ts.strftime("%H")
    return base / topic / f"dt={dt}" / f"hour={hour}"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def open_new_part(dir_path: Path, part_idx: int):
    fname = dir_path / f"part-{part_idx:06d}.jsonl"
    return open(fname, "ab")  # append binary


def safe_json_bytes(value_bytes: bytes) -> bytes:
    # If the value is already JSON bytes (it is), keep it.
    # But we want to guarantee newline delimiting and catch bad payloads.
    try:
        obj = json.loads(value_bytes.decode("utf-8"))
    except Exception:
        obj = {"_bronze_parse_error": True, "raw": value_bytes.decode("utf-8", errors="replace")}
    return orjson.dumps(obj) + b"\n"


def main():
    load_dotenv(dotenv_path=".env")

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("BRONZE_TOPIC", "trades")
    base_dir = Path(os.getenv("BRONZE_BASE_DIR", "data/bronze"))

    # Roll file every N messages OR every N seconds (whichever comes first)
    max_msgs_per_file = int(os.getenv("BRONZE_MAX_MSGS_PER_FILE", "5000"))
    max_seconds_per_file = int(os.getenv("BRONZE_MAX_SECONDS_PER_FILE", "300"))

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "bronze-writer-trades-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,   # we commit only after fsync
    })

    consumer.subscribe([topic])
    print(f"[bronze] consuming topic={topic} bootstrap={bootstrap}")
    print(f"[bronze] writing under {base_dir.resolve()}")

    part_idx = 1
    last_written_message = None
    current_bucket: Optional[Path] = None
    f = None
    msg_count = 0
    file_opened_at = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[bronze] kafka error: {msg.error()}")
                continue

            # Use ingestion time for partitioning (Bronze = as received)
            ts = now_utc()
            bucket = dt_hour_path(base_dir, topic, ts)
            if current_bucket != bucket or f is None:
                # new hour bucket (or first open)
                if f:
                    f.flush()
                    os.fsync(f.fileno())
                    f.close()
                ensure_dir(bucket)
                current_bucket = bucket
                f = open_new_part(bucket, part_idx)
                msg_count = 0
                file_opened_at = time.time()
                part_idx += 1
                print(f"[bronze] opened {f.name}")

            # write message line
            f.write(safe_json_bytes(msg.value()))
            last_written_message = msg
            msg_count += 1

            # flush & commit periodically (reliability)
            # commit offsets only after we fsync, so restart won't lose acknowledged data
            if msg_count % 200 == 0:
                f.flush()
                os.fsync(f.fileno())
                consumer.commit(message=last_written_message, asynchronous=False)

            # roll file conditions
            if msg_count >= max_msgs_per_file or (time.time() - file_opened_at) >= max_seconds_per_file:
                f.flush()
                os.fsync(f.fileno())
                consumer.commit(message=last_written_message, asynchronous=False)
                f.close()
                f = None  # force reopen new part next message

    except KeyboardInterrupt:
        print("\n[bronze] stopping...")
    finally:
        try:
            if f:
                f.flush()
                os.fsync(f.fileno())
                f.close()
                consumer.commit(message=last_written_message, asynchronous=False)
        finally:
            consumer.close()
            print("[bronze] closed")


if __name__ == "__main__":
    main()

from datetime import datetime, timezone
import json
import os
import glob
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def parse_ingested_at(x: str) -> pd.Timestamp:
    # "2026-03-05T15:54:34Z"
    return pd.to_datetime(x, utc=True, errors="coerce")


def main():
    load_dotenv(dotenv_path=".env")

    bronze_base = Path(os.getenv("BRONZE_BASE_DIR", "data/bronze")) / "trades"
    silver_base = Path(os.getenv("SILVER_BASE_DIR", "data/silver")) / "trades"
    quarantine_base = Path(os.getenv("QUARANTINE_BASE_DIR", "data/quarantine")) / "trades"

    # Read all bronze files we have (later we’ll do incremental)
    files = sorted(glob.glob(str(bronze_base / "dt=*" / "hour=*" / "part-*.jsonl")))
    if not files:
        print(f"[silver] no bronze files found under {bronze_base}")
        return

    print(f"[silver] reading {len(files)} bronze files...")

    # Load JSONL into one dataframe
    df = pd.concat([pd.read_json(f, lines=True) for f in files], ignore_index=True)

    # Normalize fields
    out = pd.DataFrame({
    "event_type": "trade",
    "symbol": df["S"] if "S" in df.columns else pd.NA,
    "price": pd.to_numeric(df["p"] if "p" in df.columns else pd.NA, errors="coerce"),
    "size": pd.to_numeric(df["s"] if "s" in df.columns else pd.NA, errors="coerce").astype("Int64"),
    "trade_ts_epoch_ms": pd.to_numeric(df["t"] if "t" in df.columns else pd.NA, errors="coerce"),
    "source": df["_src"] if "_src" in df.columns else pd.NA,
    "ingested_at": (df["_ingested_at"] if "_ingested_at" in df.columns else pd.NA).map(parse_ingested_at),
    })

    # Convert timestamps
    out["trade_ts"] = pd.to_datetime(out["trade_ts_epoch_ms"], unit="ms", utc=True, errors="coerce")
    out["dt"] = out["trade_ts"].dt.date.astype("string")

    # Data quality checks
    bad = (
        out["symbol"].isna()
        | out["trade_ts"].isna()
        | out["ingested_at"].isna()
        | out["price"].isna()
        | (out["price"] <= 0)
        | out["size"].isna()
        | (out["size"] < 0)
    )

    bad_df = out[bad].copy()
    good_df = out[~bad].copy()

    # Quarantine bad rows
    if len(bad_df) > 0:
        quarantine_base.mkdir(parents=True, exist_ok=True)
        q_path = quarantine_base / f"bad_trades_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet"
        pq.write_table(pa.Table.from_pandas(bad_df, preserve_index=False), q_path)
        print(f"[silver] quarantined {len(bad_df)} bad rows -> {q_path}")

    # Build dedupe key
    good_df["event_id"] = (
        good_df["symbol"].astype(str) + "|"
        + good_df["trade_ts_epoch_ms"].astype("Int64").astype(str) + "|"
        + good_df["price"].round(6).astype(str) + "|"
        + good_df["size"].astype("Int64").astype(str)
    )

    before = len(good_df)
    good_df = good_df.drop_duplicates(subset=["event_id"])
    after = len(good_df)
    print(f"[silver] deduped: {before} -> {after} rows")

    # --- quality report (upgrade) ---
    report = {
        "run_ts_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "bronze_files": len(files),
        "rows_in": int(len(out)),
        "rows_quarantined": int(len(bad_df)),
        "rows_good_pre_dedupe": int(before),
        "rows_good_post_dedupe": int(after),
    }
    Path("data/gold/quality_reports").mkdir(parents=True, exist_ok=True)
    report_path = Path("data/gold/quality_reports") / f"silver_trades_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    report_path.write_text(json.dumps(report, indent=2))
    print("[silver] quality_report:", json.dumps(report, indent=2))
    print(f"[silver] quality_report_saved -> {report_path}")

    # Write Parquet partitioned by dt and symbol
    silver_base.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(good_df, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=str(silver_base),
        partition_cols=["dt", "symbol"],
        existing_data_behavior="overwrite_or_ignore",
    )

    print(f"[silver] wrote parquet dataset -> {silver_base.resolve()}")


if __name__ == "__main__":
    main()

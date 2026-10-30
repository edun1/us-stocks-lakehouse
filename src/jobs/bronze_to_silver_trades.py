"""Validate Bronze trades and rebuild affected Silver partitions."""

import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def main():
    load_dotenv()

    bronze = Path(os.getenv("BRONZE_BASE_DIR", "data/bronze")) / "trades"
    silver = Path(os.getenv("SILVER_BASE_DIR", "data/silver")) / "trades"
    quarantine = (
        Path(os.getenv("QUARANTINE_BASE_DIR", "data/quarantine")) / "trades"
    )

    files = sorted(bronze.glob("dt=*/hour=*/part-*.jsonl"))
    if not files:
        raise SystemExit(f"No Bronze files found under {bronze}")

    raw = pd.concat(
        [pd.read_json(path, lines=True) for path in files],
        ignore_index=True,
    )

    # Missing columns become null and fail validation.
    raw = raw.reindex(
        columns=["S", "p", "s", "t", "_src", "_ingested_at"]
    )

    price = pd.to_numeric(raw["p"], errors="coerce")
    size = pd.to_numeric(raw["s"], errors="coerce")
    epoch = pd.to_numeric(raw["t"], errors="coerce")

    out = pd.DataFrame({
        "symbol": raw["S"].astype("string").str.strip().str.upper(),
        "price": price,
        "size": size,
        "trade_ts_epoch_ms": epoch,
        "source": raw["_src"].astype("string"),
        "ingested_at": pd.to_datetime(
            raw["_ingested_at"], utc=True, errors="coerce"
        ),
    })

    out["trade_ts"] = pd.to_datetime(
        epoch, unit="ms", utc=True, errors="coerce"
    )

    valid = (
        out["symbol"].notna()
        & out["symbol"].ne("")
        & price.notna()
        & price.gt(0)
        & ~price.isin([float("inf"), float("-inf")])
        & size.notna()
        & size.gt(0)
        & size.lt(2**63)
        & size.mod(1).eq(0)
        & epoch.mod(1).eq(0)
        & out["trade_ts"].notna()
        & out["ingested_at"].notna()
        & out["source"].notna()
        & out["source"].ne("")
    ).fillna(False)

    rejected = raw.loc[~valid].copy()
    rejected["reason"] = "INVALID_TRADE"

    quarantine.mkdir(parents=True, exist_ok=True)
    rejected.to_json(
        quarantine / "latest_rejected.jsonl",
        orient="records",
        lines=True,
    )

    good = out.loc[valid].copy()
    if good.empty:
        raise SystemExit("No valid trades; existing Silver data was preserved.")

    good["size"] = good["size"].astype("int64")
    good["trade_ts_epoch_ms"] = good["trade_ts_epoch_ms"].astype("int64")
    good["dt"] = good["trade_ts"].dt.strftime("%Y-%m-%d")

    # Composite key is a fallback when the feed has no unique trade ID.
    good["event_id"] = (
        good["source"] + "|"
        + good["symbol"] + "|"
        + good["trade_ts_epoch_ms"].astype(str) + "|"
        + good["price"].astype(str) + "|"
        + good["size"].astype(str)
    )

    before = len(good)
    good = good.drop_duplicates("event_id")
    good = good.sort_values(["symbol", "trade_ts", "event_id"])

    report = {
        "bronze_files": len(files),
        "rows_in": len(raw),
        "rows_quarantined": len(rejected),
        "rows_good_pre_dedupe": before,
        "duplicates_removed": before - len(good),
        "rows_good_post_dedupe": len(good),
    }

    assert len(raw) == len(rejected) + before

    silver.mkdir(parents=True, exist_ok=True)
    pq.write_to_dataset(
        pa.Table.from_pandas(good, preserve_index=False),
        root_path=str(silver),
        partition_cols=["dt", "symbol"],
        basename_template="part-{i}.parquet",
        existing_data_behavior="delete_matching",
    )

    report_dir = Path("data/gold/quality_reports")
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "silver_latest.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
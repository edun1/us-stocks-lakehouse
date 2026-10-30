"""Build per-minute financial features from Silver trades."""

import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def main():
    load_dotenv()

    silver = Path(os.getenv("SILVER_BASE_DIR", "data/silver")) / "trades"
    gold = Path("data/gold/features_trades")

    df = pd.read_parquet(silver)
    if df.empty:
        raise SystemExit("Silver contains no trades.")

    required = {"symbol", "price", "size", "trade_ts", "event_id"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    if df["event_id"].duplicated().any():
        raise ValueError("Duplicate Silver event IDs detected.")

    df["symbol"] = df["symbol"].astype("string")
    df["trade_ts"] = pd.to_datetime(df["trade_ts"], utc=True)
    df = df.sort_values(["symbol", "trade_ts", "event_id"])

    df["minute_ts"] = df["trade_ts"].dt.floor("min")
    df["dt"] = df["minute_ts"].dt.strftime("%Y-%m-%d")
    df["price_times_size"] = df["price"] * df["size"]

    keys = ["dt", "symbol", "minute_ts"]

    out = df.groupby(keys, observed=True, as_index=False).agg(
        weighted_value=("price_times_size", "sum"),
        volume=("size", "sum"),
        trade_count=("event_id", "count"),
        price_last=("price", "last"),
    )

    if out["volume"].le(0).any():
        raise ValueError("Nonpositive minute volume detected.")

    out["vwap"] = out["weighted_value"] / out["volume"]
    out = out.sort_values(["symbol", "minute_ts"]).reset_index(drop=True)

    # Reset features at each UTC date boundary.
    groups = out.groupby(["symbol", "dt"], observed=True)

    previous_price = groups["vwap"].shift(1)
    previous_minute = groups["minute_ts"].shift(1)

    # Only calculate a one-minute return for adjacent minutes.
    adjacent = (
        out["minute_ts"] - previous_minute
    ).eq(pd.Timedelta(minutes=1))

    out["return_1m"] = (
        out["vwap"] / previous_price - 1
    ).where(adjacent)

    # Five consecutive one-minute returns are required.
    # A missing minute makes its return null, breaking the window.
    out["vol_5m"] = out.groupby(
        ["symbol", "dt"], observed=True
    )["return_1m"].transform(
        lambda series: series.rolling(
            window=5, min_periods=5
        ).std(ddof=1)
    )

    # Reconcile Gold totals to Silver before writing.
    assert int(out["trade_count"].sum()) == len(df)
    assert int(out["volume"].sum()) == int(df["size"].sum())
    assert not out.duplicated(["symbol", "minute_ts"]).any()

    columns = [
        "dt", "symbol", "minute_ts",
        "vwap", "price_last", "volume", "trade_count",
        "return_1m", "vol_5m",
    ]
    out = out[columns]

    gold.mkdir(parents=True, exist_ok=True)
    pq.write_to_dataset(
        pa.Table.from_pandas(out, preserve_index=False),
        root_path=str(gold),
        partition_cols=["dt", "symbol"],
        basename_template="part-{i}.parquet",
        existing_data_behavior="delete_matching",
    )

    print(f"Silver trades: {len(df):,}")
    print(f"Gold minute rows: {len(out):,}")
    print(f"Reconciled trade count: {out['trade_count'].sum():,}")
    print("Volume reconciliation: PASS")
    print(out.head(8).to_string(index=False))


if __name__ == "__main__":
    main()
import glob
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv


def floor_to_minute(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("min")


def main():
    load_dotenv(dotenv_path=".env")

    silver_root = Path("data/silver/trades")
    gold_root = Path("data/gold/features_trades")

    # Read the whole dataset so partition columns (dt, symbol) are included
    df = pd.read_parquet(str(silver_root), engine="pyarrow")

    # Ensure required columns exist
    required = ["symbol", "price", "size", "trade_ts"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"[gold] missing required columns: {missing}. Columns present: {list(df.columns)}")

    # Ensure types
    df["trade_ts"] = pd.to_datetime(df["trade_ts"], utc=True, errors="coerce")
    df["minute_ts"] = floor_to_minute(df["trade_ts"])
    df["dt"] = df["minute_ts"].dt.date.astype("string")

    # Base per-minute aggregates
    # VWAP = sum(price * size) / sum(size)
    g = df.groupby(["dt", "symbol", "minute_ts"], as_index=False).agg(
        vwap_num=("price", lambda x: 0.0),  # placeholder (we’ll compute below)
        volume=("size", "sum"),
        trade_count=("event_id", "count"),
        price_last=("price", "last"),
    )

    # Compute VWAP properly using price*size
    df["px_sz"] = df["price"] * df["size"].astype(float)
    vwap = df.groupby(["dt", "symbol", "minute_ts"], as_index=False).agg(
        vwap_num=("px_sz", "sum"),
        vwap_den=("size", "sum"),
    )
    vwap["vwap"] = vwap["vwap_num"] / vwap["vwap_den"].replace({0: pd.NA})

    # Merge aggregates
    out = g.drop(columns=["vwap_num"]).merge(
        vwap[["dt", "symbol", "minute_ts", "vwap"]],
        on=["dt", "symbol", "minute_ts"],
        how="left",
    )

    # Sort for time-series features
    out = out.sort_values(["symbol", "minute_ts"]).reset_index(drop=True)

    # Returns: pct change in vwap (fallback to last price if vwap missing)
    base_price = out["vwap"].fillna(out["price_last"])
    out["return_1m"] = base_price.groupby(out["symbol"]).pct_change()

    # Rolling volatility (5 minutes) on return_1m
    out["vol_5m"] = (
        out.groupby("symbol")["return_1m"]
        .rolling(window=5, min_periods=3)
        .std()
        .reset_index(level=0, drop=True)
    )

    # Clean up final columns
    out = out[[
        "dt", "symbol", "minute_ts",
        "vwap", "price_last", "volume", "trade_count",
        "return_1m", "vol_5m"
    ]]

    gold_root.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(out, preserve_index=False)

    pq.write_to_dataset(
        table,
        root_path=str(gold_root),
        partition_cols=["dt", "symbol"],
        existing_data_behavior="overwrite_or_ignore",
    )

    print(f"[gold] wrote features dataset -> {gold_root.resolve()}")
    print("[gold] sample:")
    print(out.head(10))


if __name__ == "__main__":
    main()

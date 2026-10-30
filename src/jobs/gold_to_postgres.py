"""Load the Gold snapshot into PostgreSQL."""

import os

import pandas as pd
import psycopg
from dotenv import load_dotenv


def main():
    load_dotenv(dotenv_path=".env")

    columns = [
        "dt", "symbol", "minute_ts",
        "vwap", "price_last", "volume", "trade_count",
        "return_1m", "vol_5m",
    ]

    df = pd.read_parquet("data/gold/features_trades")
    df = df[columns].copy()

    if df.empty:
        raise ValueError("Gold is empty; existing database data was preserved.")

    if df.duplicated(["symbol", "minute_ts"]).any():
        raise ValueError("Duplicate symbol/minute pairs found in Gold.")

    df["minute_ts"] = pd.to_datetime(df["minute_ts"], utc=True)

    # Convert missing feature values to SQL NULL.
    df = df.astype(object).where(pd.notna(df), None)
    rows = list(df.itertuples(index=False, name=None))

    print(f"[pg] rows to load: {len(rows)}")

    with psycopg.connect(
        host="localhost",
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS features_trades (
                    dt date,
                    symbol text,
                    minute_ts timestamptz,
                    vwap double precision,
                    price_last double precision,
                    volume bigint,
                    trade_count bigint,
                    return_1m double precision,
                    vol_5m double precision
                )
            """)

            # Replace this demo snapshot within one transaction.
            cur.execute("TRUNCATE TABLE features_trades")

            cur.executemany("""
                INSERT INTO features_trades (
                    dt, symbol, minute_ts,
                    vwap, price_last, volume, trade_count,
                    return_1m, vol_5m
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, rows)

            cur.execute("SELECT COUNT(*) FROM features_trades")
            stored = cur.fetchone()[0]

            if stored != len(rows):
                raise ValueError("Database row-count reconciliation failed.")

    print(f"[pg] loaded table: features_trades ({stored} rows)")
    print("[pg] done")


if __name__ == "__main__":
    main()
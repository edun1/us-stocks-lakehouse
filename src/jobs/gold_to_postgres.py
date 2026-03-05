import os
import pandas as pd
import psycopg
from dotenv import load_dotenv

TABLE = "features_trades"

def main():
    load_dotenv(dotenv_path=".env")

    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "stocks")
    conn_str = f"postgresql://{user}:{pw}@localhost:5432/{db}"

    df = pd.read_parquet("data/gold/features_trades", engine="pyarrow")

    # Ensure expected columns exist
    cols = ["dt","symbol","minute_ts","vwap","price_last","volume","trade_count","return_1m","vol_5m"]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"[pg] missing columns: {missing}. Present: {list(df.columns)}")

    # Types + NULL handling
    df["minute_ts"] = pd.to_datetime(df["minute_ts"], utc=True, errors="coerce")

    # Force everything to plain Python objects, then replace missing with None
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    print(f"[pg] rows to load: {len(df)}")

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
              dt date,
              symbol text,
              minute_ts timestamptz,
              vwap double precision,
              price_last double precision,
              volume bigint,
              trade_count bigint,
              return_1m double precision,
              vol_5m double precision
            );
            """)
            cur.execute(f"TRUNCATE TABLE {TABLE};")

            insert_sql = f"""
            INSERT INTO {TABLE} ({",".join(cols)})
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.executemany(insert_sql, [tuple(r) for r in df[cols].itertuples(index=False, name=None)])
        conn.commit()

    print(f"[pg] loaded table: {TABLE}")
    print("[pg] done")

if __name__ == "__main__":
    main()
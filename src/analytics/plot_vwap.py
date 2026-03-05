import os
import pandas as pd
import matplotlib.pyplot as plt
import psycopg
from dotenv import load_dotenv


def main():
    load_dotenv(dotenv_path=".env")

    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "stocks")
    conn_str = f"postgresql://{user}:{pw}@localhost:5432/{db}"

    symbol = os.getenv("PLOT_SYMBOL", "AAPL")
    limit = int(os.getenv("PLOT_LIMIT", "500"))

    query = """
    SELECT minute_ts, vwap
    FROM features_trades
    WHERE symbol = %s
    ORDER BY minute_ts ASC
    LIMIT %s;
    """

    with psycopg.connect(conn_str) as conn:
        df = pd.read_sql(query, conn, params=(symbol, limit))

    if df.empty:
        raise SystemExit(f"No rows found for symbol={symbol}. Did you load Postgres?")

    df["minute_ts"] = pd.to_datetime(df["minute_ts"], utc=True)

    # Plot
    plt.figure()
    plt.plot(df["minute_ts"], df["vwap"])
    plt.title(f"VWAP (1m) — {symbol}")
    plt.xlabel("Time (UTC)")
    plt.ylabel("VWAP")
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_path = f"docs/screenshots/vwap_{symbol.lower()}.png"
    plt.savefig(out_path, dpi=160)
    print(f"[plot] saved -> {out_path}")


if __name__ == "__main__":
    main()

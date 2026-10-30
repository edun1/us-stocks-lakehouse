# Verified Local Replay

This extension adds reproducible synthetic trades to the original
US Stocks Lakehouse project by Pattarin Thunyapar.

## Verified pipeline

Synthetic trades → Kafka trades_replay → Bronze JSONL →
Silver Parquet → Gold financial features → PostgreSQL

Silver and Gold processing run as batch jobs after ingestion.

## Results

- 1,800 synthetic trades across AAPL, MSFT, and NVDA.
- 30 minutes of data per symbol.
- 1,800 unique Silver records after repeated processing.
- 90 Gold records with no duplicate symbol/minute pairs.
- PostgreSQL: 30 minute records and 600 trades per symbol.
- Gold trade counts and volume reconciled to Silver.
- Two automated tests passed.

## Improvements

- Added a deterministic sample generator and Kafka replay publisher.
- Added missing Python dependencies.
- Replaced affected Silver and Gold partitions on reruns.
- Sorted trades by event time before selecting the last price.
- Calculated one-minute returns only across adjacent minutes.
- Required five consecutive returns for rolling volatility.
- Passed PostgreSQL credentials as separate connection parameters.
- Added an explicit last-message offset to consumer commit calls.
  This shutdown change still needs a runtime retest.

## Run the tests

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

## Scope and limitations

The verified data is synthetic, not live market data.
Live-provider ingestion has not been validated in this extension.

Storage uses plain Parquet, without a transactional lakehouse table format.
Affected-partition replacement is not an atomic full-dataset transaction.
Run transformation jobs one at a time.

Deduplication uses a composite event key. Distinct real trades with
identical key fields could be collapsed.

Financial features reset at UTC date boundaries, not exchange sessions.
Volatility is not annualized.

PostgreSQL loading replaces the feature-table snapshot in one transaction.

The original project's MIT license and copyright notice are retained.
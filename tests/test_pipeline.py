import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]


class PipelineTests(unittest.TestCase):
    def make_records(self):
        start = pd.Timestamp("2026-01-05T14:30:00Z")

        def trade(minute, second, price, size):
            timestamp = start + pd.Timedelta(
                minutes=minute, seconds=second
            )
            return {
                "T": "t",
                "S": "TEST",
                "p": price,
                "s": size,
                "t": int(timestamp.timestamp() * 1000),
                "_src": "test",
                "_ingested_at": timestamp.isoformat(),
            }

        records = [
            trade(0, 0, 100.0, 10),
            trade(0, 10, 110.0, 30),
        ]

        for minute in range(1, 7):
            records.append(
                trade(minute, 0, 107.5 * 1.01**minute, 5)
            )

        # Reverse arrival order to test event-time sorting.
        return list(reversed(records))

    def run_jobs(self, folder, records):
        folder = Path(folder)
        bronze = folder / "data/bronze/trades/dt=2026-01-05/hour=14"
        bronze.mkdir(parents=True, exist_ok=True)

        sample = bronze / "part-test.jsonl"
        sample.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )

        env = os.environ.copy()
        env.update({
            "BRONZE_BASE_DIR": str(folder / "data/bronze"),
            "BRONZE_TOPIC": "trades",
            "SILVER_BASE_DIR": str(folder / "data/silver"),
            "QUARANTINE_BASE_DIR": str(folder / "data/quarantine"),
        })

        for script in [
            "bronze_to_silver_trades.py",
            "silver_to_gold_features.py",
        ]:
            result = subprocess.run(
                [sys.executable, str(ROOT / "src/jobs" / script)],
                cwd=folder,
                env=env,
                capture_output=True,
                text=True,
            )
            self.assertEqual(
                result.returncode,
                0,
                result.stdout + result.stderr,
            )

        silver = pd.read_parquet(folder / "data/silver/trades")
        gold = pd.read_parquet(folder / "data/gold/features_trades")
        report = json.loads(
            (folder / "data/gold/quality_reports/silver_latest.json")
            .read_text(encoding="utf-8")
        )
        return silver, gold.sort_values("minute_ts"), report

    def test_calculations_quality_and_replay(self):
        records = self.make_records()
        duplicate = dict(records[-1])
        invalid = dict(records[-1], p=-1)
        records += [duplicate, invalid]

        with tempfile.TemporaryDirectory() as folder:
            silver, gold, report = self.run_jobs(folder, records)

            self.assertEqual(len(silver), 8)
            self.assertEqual(len(gold), 7)
            self.assertEqual(report["rows_quarantined"], 1)
            self.assertEqual(report["duplicates_removed"], 1)

            first = gold.iloc[0]
            self.assertAlmostEqual(first["vwap"], 107.5)
            self.assertAlmostEqual(first["price_last"], 110.0)
            self.assertEqual(int(first["volume"]), 40)

            self.assertAlmostEqual(gold.iloc[1]["return_1m"], 0.01)
            self.assertTrue(gold.iloc[:5]["vol_5m"].isna().all())
            self.assertAlmostEqual(gold.iloc[5]["vol_5m"], 0.0)

            self.assertEqual(int(gold["trade_count"].sum()), 8)
            self.assertEqual(
                int(gold["volume"].sum()), int(silver["size"].sum())
            )

            # Rerun both jobs and inspect the stored data again.
            silver_again, gold_again, _ = self.run_jobs(folder, records)
            self.assertEqual(len(silver_again), 8)
            self.assertEqual(len(gold_again), 7)
            self.assertFalse(silver_again["event_id"].duplicated().any())
            self.assertFalse(
                gold_again.duplicated(["symbol", "minute_ts"]).any()
            )

    def test_missing_minute_breaks_return_window(self):
        records = [
            record for record in self.make_records()
            if pd.to_datetime(record["t"], unit="ms", utc=True).minute != 33
        ]

        with tempfile.TemporaryDirectory() as folder:
            _, gold, _ = self.run_jobs(folder, records)

            after_gap = gold[
                gold["minute_ts"].dt.minute.eq(34)
            ].iloc[0]

            self.assertTrue(pd.isna(after_gap["return_1m"]))
            self.assertTrue(gold["vol_5m"].isna().all())


if __name__ == "__main__":
    unittest.main()
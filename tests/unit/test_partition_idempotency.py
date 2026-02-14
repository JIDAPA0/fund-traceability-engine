from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipelines.run_build_mart import _write_partition as write_mart_partition
from pipelines.run_build_staging import _write_partition as write_staging_partition


class TestPartitionIdempotency(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "test.db"
        self.engine = create_engine(f"sqlite:///{self.db_path}")

    def tearDown(self) -> None:
        self.engine.dispose()
        self.tmp.cleanup()

    def _count(self, table_name: str, as_of_date: str | None = None) -> int:
        if as_of_date is None:
            query = text(f"SELECT COUNT(*) FROM {table_name}")
            params = {}
        else:
            query = text(f"SELECT COUNT(*) FROM {table_name} WHERE as_of_date = :as_of_date")
            params = {"as_of_date": as_of_date}

        with self.engine.connect() as conn:
            return int(conn.execute(query, params).scalar_one())

    def test_staging_write_is_idempotent_per_as_of_date(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE stg_funds (
                      fund_id TEXT,
                      fund_name TEXT,
                      source TEXT,
                      currency TEXT,
                      as_of_date TEXT
                    )
                    """
                )
            )

        first = pd.DataFrame(
            [
                {
                    "fund_id": "F1",
                    "fund_name": "Fund 1",
                    "source": "global",
                    "currency": "USD",
                    "as_of_date": "2026-02-14",
                },
                {
                    "fund_id": "F2",
                    "fund_name": "Fund 2",
                    "source": "global",
                    "currency": "USD",
                    "as_of_date": "2026-02-14",
                },
            ]
        )
        self.assertEqual(write_staging_partition(self.engine, "stg_funds", "2026-02-14", first), 2)
        self.assertEqual(self._count("stg_funds", "2026-02-14"), 2)

        rerun_same_day = pd.DataFrame(
            [
                {
                    "fund_id": "F3",
                    "fund_name": "Fund 3",
                    "source": "global",
                    "currency": "USD",
                    "as_of_date": "2026-02-14",
                }
            ]
        )
        self.assertEqual(write_staging_partition(self.engine, "stg_funds", "2026-02-14", rerun_same_day), 1)
        self.assertEqual(self._count("stg_funds", "2026-02-14"), 1)

        next_day = rerun_same_day.copy()
        next_day["as_of_date"] = "2026-02-15"
        self.assertEqual(write_staging_partition(self.engine, "stg_funds", "2026-02-15", next_day), 1)
        self.assertEqual(self._count("stg_funds"), 2)
        self.assertEqual(self._count("stg_funds", "2026-02-14"), 1)
        self.assertEqual(self._count("stg_funds", "2026-02-15"), 1)

    def test_mart_write_is_idempotent_per_as_of_date(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE mart_true_exposure (
                      root_fund_id TEXT,
                      final_asset_id TEXT,
                      effective_weight REAL,
                      path_depth INTEGER,
                      as_of_date TEXT
                    )
                    """
                )
            )

        first = pd.DataFrame(
            [
                {
                    "root_fund_id": "TH_FEEDER_MAIN",
                    "final_asset_id": "EQ_US_TECH",
                    "effective_weight": 0.42,
                    "path_depth": 4,
                },
                {
                    "root_fund_id": "TH_FEEDER_MAIN",
                    "final_asset_id": "BOND_GOV_10Y",
                    "effective_weight": 0.30,
                    "path_depth": 3,
                },
            ]
        )
        self.assertEqual(write_mart_partition(self.engine, "2026-02-14", first), 2)
        self.assertEqual(self._count("mart_true_exposure", "2026-02-14"), 2)

        rerun_same_day = pd.DataFrame(
            [
                {
                    "root_fund_id": "TH_FEEDER_MAIN",
                    "final_asset_id": "EQ_EU_BLUECHIP",
                    "effective_weight": 0.28,
                    "path_depth": 4,
                }
            ]
        )
        self.assertEqual(write_mart_partition(self.engine, "2026-02-14", rerun_same_day), 1)
        self.assertEqual(self._count("mart_true_exposure", "2026-02-14"), 1)

        self.assertEqual(write_mart_partition(self.engine, "2026-02-15", rerun_same_day), 1)
        self.assertEqual(self._count("mart_true_exposure"), 2)
        self.assertEqual(self._count("mart_true_exposure", "2026-02-14"), 1)
        self.assertEqual(self._count("mart_true_exposure", "2026-02-15"), 1)


if __name__ == "__main__":
    unittest.main()

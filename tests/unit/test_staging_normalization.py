from __future__ import annotations

from pathlib import Path
import sys
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipelines.run_build_staging import _normalize_holdings, _normalize_links


class TestStagingNormalization(unittest.TestCase):
    def test_normalize_holdings_drops_missing_asset_id(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "fund_id": "F_MASTER_A",
                    "asset_id": "EQ_US_TECH",
                    "asset_name": "US Tech Basket",
                    "asset_type": "equity",
                    "weight": 0.6,
                },
                {
                    "fund_id": "F_MASTER_A",
                    "asset_id": None,
                    "asset_name": "Missing asset",
                    "asset_type": "equity",
                    "weight": 0.1,
                },
            ]
        )

        normalized = _normalize_holdings(raw, "2026-02-14", known_fund_ids={"F_MASTER_A"})

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized.iloc[0]["asset_id"], "EQ_US_TECH")

    def test_normalize_links_drops_missing_master_id(self) -> None:
        raw = pd.DataFrame(
            [
                {"feeder_fund_id": "TH_FEEDER_MAIN", "master_fund_id": "F_MASTER_C", "confidence": 1.0},
                {"feeder_fund_id": "TH_FEEDER_BROKEN", "master_fund_id": None, "confidence": 0.8},
            ]
        )

        normalized = _normalize_links(raw, "2026-02-14")

        self.assertEqual(len(normalized), 1)
        self.assertEqual(normalized.iloc[0]["feeder_fund_id"], "TH_FEEDER_MAIN")


if __name__ == "__main__":
    unittest.main()

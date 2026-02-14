from __future__ import annotations

from pathlib import Path
import sys
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipelines.run_build_mart import _compute_true_exposure


class TestExposureCalculation(unittest.TestCase):
    def _sample_holdings(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"fund_id": "F_MASTER_A", "asset_id": "EQ_US_TECH", "asset_type": "equity", "weight": 0.60},
                {
                    "fund_id": "F_MASTER_A",
                    "asset_id": "EQ_EU_BLUECHIP",
                    "asset_type": "equity",
                    "weight": 0.40,
                },
                {"fund_id": "F_MASTER_B", "asset_id": "F_MASTER_A", "asset_type": "fund", "weight": 0.70},
                {"fund_id": "F_MASTER_B", "asset_id": "BOND_GOV_10Y", "asset_type": "bond", "weight": 0.30},
                {"fund_id": "F_MASTER_C", "asset_id": "F_MASTER_B", "asset_type": "fund", "weight": 1.00},
                {"fund_id": "F_CYCLE_1", "asset_id": "F_CYCLE_2", "asset_type": "fund", "weight": 1.00},
                {"fund_id": "F_CYCLE_2", "asset_id": "F_CYCLE_1", "asset_type": "fund", "weight": 1.00},
            ]
        )

    def _sample_links(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"feeder_fund_id": "TH_FEEDER_MAIN", "master_fund_id": "F_MASTER_C", "confidence": 1.0},
                {"feeder_fund_id": "TH_FEEDER_HALF", "master_fund_id": "F_MASTER_B", "confidence": 0.5},
                {"feeder_fund_id": "TH_FEEDER_CYCLE", "master_fund_id": "F_CYCLE_1", "confidence": 1.0},
            ]
        )

    def _get_exposure(self, df: pd.DataFrame, root: str, asset: str) -> tuple[float, int]:
        row = df[(df["root_fund_id"] == root) & (df["final_asset_id"] == asset)]
        self.assertEqual(len(row), 1)
        return float(row.iloc[0]["effective_weight"]), int(row.iloc[0]["path_depth"])

    def test_multilayer_exposure_with_confidence(self) -> None:
        result = _compute_true_exposure(self._sample_holdings(), self._sample_links(), max_depth=6)

        eq_us, eq_us_depth = self._get_exposure(result, "TH_FEEDER_MAIN", "EQ_US_TECH")
        eq_eu, eq_eu_depth = self._get_exposure(result, "TH_FEEDER_MAIN", "EQ_EU_BLUECHIP")
        bond, bond_depth = self._get_exposure(result, "TH_FEEDER_MAIN", "BOND_GOV_10Y")

        self.assertAlmostEqual(eq_us, 0.42, places=9)
        self.assertAlmostEqual(eq_eu, 0.28, places=9)
        self.assertAlmostEqual(bond, 0.30, places=9)
        self.assertEqual(eq_us_depth, 4)
        self.assertEqual(eq_eu_depth, 4)
        self.assertEqual(bond_depth, 3)

        half_eq_us, _ = self._get_exposure(result, "TH_FEEDER_HALF", "EQ_US_TECH")
        half_eq_eu, _ = self._get_exposure(result, "TH_FEEDER_HALF", "EQ_EU_BLUECHIP")
        half_bond, _ = self._get_exposure(result, "TH_FEEDER_HALF", "BOND_GOV_10Y")

        self.assertAlmostEqual(half_eq_us, 0.21, places=9)
        self.assertAlmostEqual(half_eq_eu, 0.14, places=9)
        self.assertAlmostEqual(half_bond, 0.15, places=9)

    def test_cycle_does_not_infinite_loop(self) -> None:
        result = _compute_true_exposure(self._sample_holdings(), self._sample_links(), max_depth=6)
        cycle_weight, cycle_depth = self._get_exposure(result, "TH_FEEDER_CYCLE", "F_CYCLE_1")

        self.assertAlmostEqual(cycle_weight, 1.0, places=9)
        self.assertEqual(cycle_depth, 3)
        self.assertLess(len(result), 100)


if __name__ == "__main__":
    unittest.main()

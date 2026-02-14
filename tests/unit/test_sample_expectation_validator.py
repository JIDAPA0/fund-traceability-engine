from __future__ import annotations

from pathlib import Path
import sys
import unittest

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from pipelines.run_validate_sample_expectation import _validate


class TestSampleExpectationValidator(unittest.TestCase):
    def _expected_df(self) -> pd.DataFrame:
        return pd.DataFrame(
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

    def test_validate_passes_when_data_matches(self) -> None:
        expected = self._expected_df()
        actual = expected.copy()
        passed, messages = _validate(expected, actual, tolerance=1e-9)

        self.assertTrue(passed)
        self.assertEqual(messages, [])

    def test_validate_detects_missing_extra_and_mismatch(self) -> None:
        expected = self._expected_df()
        actual = pd.DataFrame(
            [
                {
                    "root_fund_id": "TH_FEEDER_MAIN",
                    "final_asset_id": "EQ_US_TECH",
                    "effective_weight": 0.40,
                    "path_depth": 2,
                },
                {
                    "root_fund_id": "TH_FEEDER_MAIN",
                    "final_asset_id": "UNEXPECTED_ASSET",
                    "effective_weight": 0.01,
                    "path_depth": 1,
                },
            ]
        )

        passed, messages = _validate(expected, actual, tolerance=1e-9)

        self.assertFalse(passed)
        joined = "\n".join(messages)
        self.assertIn("Missing rows in actual", joined)
        self.assertIn("Unexpected rows in actual", joined)
        self.assertIn("Weight mismatch", joined)
        self.assertIn("Depth mismatch", joined)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from transform.normalize.canonical_ids import canonical_id
from transform.normalize.currency_normalizer import normalize_currency
from transform.normalize.ticker_normalizer import normalize_ticker


class TestNormalizers(unittest.TestCase):
    def test_currency_alias_and_passthrough(self) -> None:
        self.assertEqual(normalize_currency("baht"), "THB")
        self.assertEqual(normalize_currency(" Dollar "), "USD")
        self.assertEqual(normalize_currency("eur"), "EUR")

    def test_ticker_normalization(self) -> None:
        self.assertEqual(normalize_ticker(" bbl .bk "), "BBL")
        self.assertEqual(normalize_ticker("spy"), "SPY")
        self.assertEqual(normalize_ticker(" BRK.B "), "BRK.B")

    def test_canonical_id_is_stable_and_case_insensitive(self) -> None:
        left = canonical_id(" Fund A ", "USD", "X")
        right = canonical_id("fund a", "usd", "x")
        self.assertEqual(left, right)
        self.assertEqual(len(left), 40)


if __name__ == "__main__":
    unittest.main()

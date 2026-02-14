"""Load bundled sample CSV files into global raw tables for local demo runs."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

import pandas as pd

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_global_raw_engine  # noqa: E402

DEFAULT_SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "samples"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load sample raw CSVs into global raw database.")
    parser.add_argument(
        "--samples-dir",
        default=str(DEFAULT_SAMPLES_DIR),
        help="Directory containing sample CSV files.",
    )
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Snapshot date written to sample rows (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--if-exists",
        choices=["replace", "append"],
        default="replace",
        help="Write mode for raw tables (default: replace).",
    )
    return parser.parse_args()


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Sample file not found: {path}")
    return pd.read_csv(path)


def _attach_as_of_date(df: pd.DataFrame, as_of_date: str) -> pd.DataFrame:
    out = df.copy()
    out["as_of_date"] = as_of_date
    return out


def main() -> int:
    args = _parse_args()
    samples_dir = Path(args.samples_dir)

    raw_funds_path = samples_dir / "raw_funds_sample.csv"
    raw_holdings_path = samples_dir / "raw_holdings_sample_with_edge_cases.csv"
    raw_links_path = samples_dir / "raw_fund_links_sample_with_edge_cases.csv"

    funds_df = _attach_as_of_date(_read_csv(raw_funds_path), args.as_of_date)
    holdings_df = _attach_as_of_date(_read_csv(raw_holdings_path), args.as_of_date)
    links_df = _attach_as_of_date(_read_csv(raw_links_path), args.as_of_date)

    engine = create_global_raw_engine()
    funds_df.to_sql("raw_funds", engine, if_exists=args.if_exists, index=False, method="multi")
    holdings_df.to_sql("raw_holdings", engine, if_exists=args.if_exists, index=False, method="multi")
    links_df.to_sql("raw_fund_links", engine, if_exists=args.if_exists, index=False, method="multi")

    print(
        "run_load_samples_to_raw completed",
        f"samples_dir={samples_dir}",
        f"as_of_date={args.as_of_date}",
        f"if_exists={args.if_exists}",
        f"rows(raw_funds)={len(funds_df)}",
        f"rows(raw_holdings)={len(holdings_df)}",
        f"rows(raw_fund_links)={len(links_df)}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Validate mart exposure output against bundled expected sample snapshot."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import sys

import pandas as pd
from sqlalchemy import inspect, text

# Allow running directly from repo root without package installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from db.connections import create_traceability_mart_engine  # noqa: E402
from utils.validation import require_columns  # noqa: E402

DEFAULT_EXPECTED_CSV = (
    Path(__file__).resolve().parents[1] / "data" / "samples" / "expected_true_exposure_sample.csv"
)

KEY_COLS = ["root_fund_id", "final_asset_id"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare mart results with expected sample exposure.")
    parser.add_argument(
        "--as-of-date",
        default=date.today().isoformat(),
        help="Partition date in mart_true_exposure to validate (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--expected-csv",
        default=str(DEFAULT_EXPECTED_CSV),
        help="Expected exposure CSV path.",
    )
    parser.add_argument(
        "--weight-tolerance",
        type=float,
        default=1e-9,
        help="Absolute tolerance for effective_weight comparisons.",
    )
    return parser.parse_args()


def _load_expected(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    require_columns(df, {"root_fund_id", "final_asset_id", "effective_weight", "path_depth"})
    df = df.copy()
    df["root_fund_id"] = df["root_fund_id"].astype(str).str.strip()
    df["final_asset_id"] = df["final_asset_id"].astype(str).str.strip()
    df["effective_weight"] = pd.to_numeric(df["effective_weight"], errors="coerce")
    df["path_depth"] = pd.to_numeric(df["path_depth"], errors="coerce").astype("Int64")
    return df


def _load_actual(as_of_date: str) -> pd.DataFrame:
    engine = create_traceability_mart_engine()
    existing = set(inspect(engine).get_table_names())
    if "mart_true_exposure" not in existing:
        return pd.DataFrame(columns=["root_fund_id", "final_asset_id", "effective_weight", "path_depth"])

    query = text(
        """
        SELECT root_fund_id, final_asset_id, effective_weight, path_depth
        FROM mart_true_exposure
        WHERE as_of_date = :as_of_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"as_of_date": as_of_date})

    if df.empty:
        return df

    df = df.copy()
    df["root_fund_id"] = df["root_fund_id"].astype(str).str.strip()
    df["final_asset_id"] = df["final_asset_id"].astype(str).str.strip()
    df["effective_weight"] = pd.to_numeric(df["effective_weight"], errors="coerce")
    df["path_depth"] = pd.to_numeric(df["path_depth"], errors="coerce").astype("Int64")
    return df


def _validate(expected_df: pd.DataFrame, actual_df: pd.DataFrame, tolerance: float) -> tuple[bool, list[str]]:
    messages: list[str] = []

    expected_roots = set(expected_df["root_fund_id"].tolist())
    actual_df = actual_df[actual_df["root_fund_id"].isin(expected_roots)].copy()

    expected_keys = set(map(tuple, expected_df[KEY_COLS].itertuples(index=False, name=None)))
    actual_keys = set(map(tuple, actual_df[KEY_COLS].itertuples(index=False, name=None)))

    missing = sorted(expected_keys - actual_keys)
    extras = sorted(actual_keys - expected_keys)

    if missing:
        messages.append(f"Missing rows in actual: {missing}")
    if extras:
        messages.append(f"Unexpected rows in actual: {extras}")

    merged = expected_df.merge(
        actual_df,
        on=KEY_COLS,
        how="inner",
        suffixes=("_expected", "_actual"),
    )

    for row in merged.itertuples(index=False):
        weight_diff = abs(float(row.effective_weight_expected) - float(row.effective_weight_actual))
        if weight_diff > tolerance:
            messages.append(
                "Weight mismatch "
                f"({row.root_fund_id}, {row.final_asset_id}): "
                f"expected={row.effective_weight_expected}, actual={row.effective_weight_actual}, diff={weight_diff}"
            )

        if int(row.path_depth_expected) != int(row.path_depth_actual):
            messages.append(
                "Depth mismatch "
                f"({row.root_fund_id}, {row.final_asset_id}): "
                f"expected={row.path_depth_expected}, actual={row.path_depth_actual}"
            )

    return len(messages) == 0, messages


def main() -> int:
    args = _parse_args()

    expected_path = Path(args.expected_csv)
    if not expected_path.exists():
        raise FileNotFoundError(f"Expected CSV not found: {expected_path}")

    expected_df = _load_expected(expected_path)
    actual_df = _load_actual(args.as_of_date)

    passed, messages = _validate(expected_df, actual_df, args.weight_tolerance)

    if passed:
        print(
            "run_validate_sample_expectation passed",
            f"as_of_date={args.as_of_date}",
            f"rows(expected)={len(expected_df)}",
            f"rows(actual_filtered)={len(actual_df[actual_df['root_fund_id'].isin(set(expected_df['root_fund_id']))])}",
        )
        return 0

    print("run_validate_sample_expectation failed", f"as_of_date={args.as_of_date}")
    for message in messages:
        print(" -", message)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Validation helpers for dataframe schemas and business rules."""

from __future__ import annotations

import pandas as pd


def require_columns(df: pd.DataFrame, required: set[str]) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

"""Core service for true exposure calculations."""

from __future__ import annotations

import pandas as pd

from transform.calc.effective_exposure import compute_effective_exposure


def compute_true_exposure(paths_df: pd.DataFrame) -> pd.DataFrame:
    return compute_effective_exposure(paths_df)

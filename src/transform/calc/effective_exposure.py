"""Compute effective exposure by recursively multiplying weights."""

from __future__ import annotations

import pandas as pd


def compute_effective_exposure(paths_df: pd.DataFrame) -> pd.DataFrame:
    if paths_df.empty:
        return pd.DataFrame(columns=["root_fund_id", "final_asset_id", "effective_weight", "path_depth"])

    grouped = (
        paths_df.groupby(["root_fund_id", "final_asset_id"], as_index=False)
        .agg(effective_weight=("path_weight", "sum"), path_depth=("depth", "max"))
    )
    return grouped

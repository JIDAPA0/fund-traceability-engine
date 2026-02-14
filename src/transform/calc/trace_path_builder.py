"""Build trace paths from feeder funds to terminal assets."""

from __future__ import annotations

import pandas as pd


def build_trace_paths(links_df: pd.DataFrame, holdings_df: pd.DataFrame) -> pd.DataFrame:
    if holdings_df.empty:
        return pd.DataFrame(columns=["root_fund_id", "final_asset_id", "path_weight", "depth"])

    paths = holdings_df.rename(
        columns={
            "fund_id": "root_fund_id",
            "asset_id": "final_asset_id",
            "weight": "path_weight",
        }
    )[["root_fund_id", "final_asset_id", "path_weight"]].copy()
    paths["depth"] = 1
    return paths

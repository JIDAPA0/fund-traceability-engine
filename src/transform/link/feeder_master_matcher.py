"""Match feeder funds to candidate master funds."""

from __future__ import annotations

import pandas as pd


def match_feeder_to_master(feeder_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    if feeder_df.empty or master_df.empty:
        return pd.DataFrame(columns=["feeder_fund_id", "master_fund_id", "confidence"])

    links = feeder_df[["fund_id"]].rename(columns={"fund_id": "feeder_fund_id"}).copy()
    links["master_fund_id"] = links["feeder_fund_id"]
    links["confidence"] = 0.0
    return links

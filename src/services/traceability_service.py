"""Core service for building fund traceability outputs."""

from __future__ import annotations

import pandas as pd

from transform.calc.trace_path_builder import build_trace_paths


def trace(feeder_holdings: pd.DataFrame, links: pd.DataFrame) -> pd.DataFrame:
    return build_trace_paths(links_df=links, holdings_df=feeder_holdings)

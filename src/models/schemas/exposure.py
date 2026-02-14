"""Schema for calculated effective exposure output."""

from dataclasses import dataclass


@dataclass(slots=True)
class Exposure:
    root_fund_id: str
    final_asset_id: str
    effective_weight: float
    path_depth: int

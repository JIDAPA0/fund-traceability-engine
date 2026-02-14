"""Schema for normalized holding rows."""

from dataclasses import dataclass


@dataclass(slots=True)
class Holding:
    fund_id: str
    asset_id: str
    weight: float
    asset_type: str

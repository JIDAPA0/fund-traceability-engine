"""Schema for feeder-master relationships."""

from dataclasses import dataclass


@dataclass(slots=True)
class FeederMasterLink:
    feeder_fund_id: str
    master_fund_id: str
    confidence: float

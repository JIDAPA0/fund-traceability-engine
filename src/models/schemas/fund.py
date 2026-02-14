"""Schema for normalized fund records."""

from dataclasses import dataclass


@dataclass(slots=True)
class Fund:
    fund_id: str
    fund_name: str
    source: str
    currency: str

"""Common enums used across extraction, linking, and exposure calculation."""

from enum import Enum


class SourceSystem(str, Enum):
    THAI = "thai"
    GLOBAL = "global"


class CurrencyCode(str, Enum):
    THB = "THB"
    USD = "USD"
    EUR = "EUR"
    HKD = "HKD"
    SGD = "SGD"


class AssetType(str, Enum):
    FUND = "fund"
    EQUITY = "equity"
    BOND = "bond"
    ETF = "etf"
    CASH = "cash"
    OTHER = "other"

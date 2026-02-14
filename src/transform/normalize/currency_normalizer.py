"""Normalize currency values to ISO-like uppercase symbols."""

from __future__ import annotations


_CURRENCY_ALIASES = {
    "BAHT": "THB",
    "THB": "THB",
    "DOLLAR": "USD",
    "USD": "USD",
}


def normalize_currency(value: str) -> str:
    return _CURRENCY_ALIASES.get(value.strip().upper(), value.strip().upper())

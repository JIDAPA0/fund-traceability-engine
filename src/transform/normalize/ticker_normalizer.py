"""Normalize ticker symbols into a canonical representation."""

from __future__ import annotations


def normalize_ticker(value: str) -> str:
    cleaned = value.strip().upper().replace(" ", "")
    return cleaned.replace(".BK", "")

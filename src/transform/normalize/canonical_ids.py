"""Build canonical IDs for funds and assets."""

from __future__ import annotations

import hashlib


def canonical_id(*parts: str) -> str:
    normalized = "|".join(part.strip().lower() for part in parts)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()

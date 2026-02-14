"""Simple retry helper for transient operations."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def retry(fn: Callable[[], T], attempts: int = 3, delay_seconds: float = 0.25) -> T:
    last_error: Exception | None = None
    for _ in range(attempts):
        try:
            return fn()
        except Exception as exc:  # intentionally broad for infrastructure calls
            last_error = exc
            time.sleep(delay_seconds)
    assert last_error is not None
    raise last_error

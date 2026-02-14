"""Build a simple bidirectional index for fund and asset search."""

from __future__ import annotations

from collections import defaultdict
from typing import DefaultDict


class SearchIndex:
    def __init__(self) -> None:
        self._forward: DefaultDict[str, set[str]] = defaultdict(set)
        self._reverse: DefaultDict[str, set[str]] = defaultdict(set)

    def add_relation(self, left: str, right: str) -> None:
        self._forward[left].add(right)
        self._reverse[right].add(left)

    def children(self, key: str) -> set[str]:
        return set(self._forward.get(key, set()))

    def parents(self, key: str) -> set[str]:
        return set(self._reverse.get(key, set()))

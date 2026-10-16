"""Business output passed to configured item pipelines."""
from __future__ import annotations

from collections.abc import Mapping


class Item:
    def __init__(self, data, *, seed=None):
        if not isinstance(data, Mapping):
            raise ValueError("Item data must be a mapping")
        self.data = dict(data)
        self.seed = seed

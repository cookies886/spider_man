"""Serializable business seed and framework execution metadata."""
from __future__ import annotations

import hashlib
import json
import math
import time
import uuid
from collections.abc import Mapping
from types import SimpleNamespace

from spider_man.utils.serialization import canonical_json, validate_name


class Seed(Mapping):
    def __init__(self, data=None, *, priority=100, project=None, task=None,
                 seed_id=None, created_at=None, execution_count=0, metadata=None):
        if not isinstance(data, Mapping):
            raise ValueError("Seed data must be a mapping")
        self.data = dict(data)
        canonical_json(self.data)
        if isinstance(priority, bool) or not isinstance(priority, (float, int)) or not math.isfinite(priority):
            raise ValueError("Seed priority must be a finite number")
        self.priority = priority
        self.project = validate_name(project, "project") if project is not None else None
        self.task = validate_name(task, "task") if task is not None else None
        if seed_id is not None and (not isinstance(seed_id, str) or not seed_id or len(seed_id) > 256):
            raise ValueError("seed_id must be a nonempty string of at most 256 characters")
        self.seed_id = seed_id or uuid.uuid4().hex
        self._explicit_id = seed_id is not None
        self.created_at = time.time() if created_at is None else float(created_at)
        if not math.isfinite(self.created_at):
            raise ValueError("created_at must be finite")
        if type(execution_count) is not int or execution_count < 0:
            raise ValueError("execution_count must be a nonnegative integer")
        self.execution_count = execution_count
        self.metadata = dict(metadata or {})
        canonical_json(self.metadata)

    @property
    def params(self):
        return SimpleNamespace(priority=self.priority)

    def __getitem__(self, key):
        return self.data[key]

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def __getattr__(self, key):
        # cobweb scripts commonly use `seed.page or 1` for optional fields.
        if key.startswith("_"):
            raise AttributeError(key)
        return self.data.get(key)

    def to_dict(self):
        return dict(self.data)

    def to_json(self):
        return canonical_json({"version": 1, "seed_id": self.seed_id, "data": self.data,
                               "priority": self.priority, "created_at": self.created_at,
                               "execution_count": self.execution_count, "project": self.project,
                               "task": self.task, "metadata": self.metadata})

    @classmethod
    def from_json(cls, value):
        body = json.loads(value)
        if not isinstance(body, dict) or type(body.get("version")) is not int or body["version"] != 1:
            raise ValueError("Unsupported Seed protocol; version 1 JSON required")
        required = {"seed_id", "data", "priority", "created_at", "execution_count"}
        if not required.issubset(body):
            raise ValueError("Incomplete Seed envelope")
        return cls(**{k: v for k, v in body.items() if k != "version"})

    def derive_id(self, parent, project, task):
        if not self._explicit_id:
            identity = canonical_json([parent.metadata.get("replay_root", parent.seed_id), project, task, self.data])
            self.seed_id = hashlib.sha256(identity.encode()).hexdigest()
        self.metadata.setdefault("parent_seed_id", parent.seed_id)
        return self

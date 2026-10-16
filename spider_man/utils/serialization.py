"""Stable serialization and shared project/task name validation."""

import json

from spider_man.exceptions import ConfigurationError


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def validate_name(value, label):
    import re
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]*", value):
        raise ConfigurationError(f"{label} must contain only letters, digits, _, . or -")
    return value

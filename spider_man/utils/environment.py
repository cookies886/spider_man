"""Safe type conversion shared by setting.py and instance configuration."""
from __future__ import annotations

import json
import math

from spider_man.exceptions import ConfigurationError


ENV_TYPES = (str, bool, int, float, list, dict, tuple)


def decode_environment_value(key, value, default, cast=None):
    try:
        if not isinstance(value, str):
            raise ValueError()
        if cast is None and default is None:
            if key in ("MONGO_UPSERT_KEYS", "DEFAULT_PROXY"):
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    if key == "DEFAULT_PROXY":
                        return value
                    raise
            return None if value == "null" else value
        kind = cast if cast is not None else type(default)
        if kind not in ENV_TYPES:
            raise ValueError()
        if kind is bool:
            normalized = value.strip().lower()
            if normalized not in ("true", "false", "1", "0"):
                raise ValueError()
            return normalized in ("true", "1")
        if kind is int:
            return int(value)
        if kind is float:
            result = float(value)
            if not math.isfinite(result):
                raise ValueError()
            return result
        if kind in (list, dict, tuple):
            result = json.loads(value)
            if not isinstance(result, list if kind is tuple else kind):
                raise ValueError()
            return tuple(result) if kind is tuple else result
        return value
    except (ValueError, TypeError):
        # Conversion exceptions can include secret values; expose the name only.
        raise ConfigurationError(f"Invalid environment setting {key}") from None

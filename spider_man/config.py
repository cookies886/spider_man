from __future__ import annotations

import importlib
import math
import os
from collections.abc import Mapping
from types import MappingProxyType
from urllib.parse import urlsplit

from . import setting as defaults
from .exceptions import ConfigurationError
from .utils.environment import ENV_TYPES as _ENV_TYPES, decode_environment_value


_CLI_SETTINGS = {"SEED_CONCURRENCY", "REQUEST_CONCURRENCY", "LOG_LEVEL", "SHUTDOWN_TIMEOUT"}
_ENV_ALIASES = {"SEED_CONCURRENCY": ("SPIDER_THREAD_NUM",)}


def import_object(path):
    if not isinstance(path, str) or "." not in path:
        raise ConfigurationError("Extension must be a dotted class path")
    module, name = path.rsplit(".", 1)
    try:
        return getattr(importlib.import_module(module), name)
    except (ImportError, AttributeError) as exc:
        raise ConfigurationError(f"Cannot load extension {path}") from exc


def _freeze(value):
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    return value


def _plain(value):
    if isinstance(value, tuple):
        return [_plain(item) for item in value]
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    return value


class Settings(Mapping):
    """Immutable Crawler configuration; explicit environment values always win.

    KEY > CLI > task > framework defaults.
    Uppercase business settings use their declared value as the environment type.
    Undeclared environment variables stay private and are read with getenv().
    """
    def __init__(self, custom_settings=None, cli=None, environ=None):
        data = dict(defaults._defaults)
        self._framework_keys = frozenset(data)
        sources = {key: "default" for key in data}
        if custom_settings is not None:
            if not isinstance(custom_settings, Mapping):
                raise ConfigurationError("custom_settings must be a mapping")
            for key, value in custom_settings.items():
                if not isinstance(key, str) or not key.isidentifier() or not key.isupper() or key.startswith("_"):
                    raise ConfigurationError("Setting names must be public uppercase identifiers")
                data[key], sources[key] = value, "task"
        # Copy before loading overrides so later process-environment changes cannot
        # change this Crawler, including after CLI applies its run parameters.
        env = dict(os.environ if environ is None else environ)
        self._environ = MappingProxyType(env)
        if "SPIDER_MAN_RUN_CONFIG" in env:
            raise ConfigurationError("SPIDER_MAN_RUN_CONFIG is unsupported; use the original setting names")
        for key, value in (cli or {}).items():
            if key not in _CLI_SETTINGS:
                raise ConfigurationError(f"Setting {key} cannot be supplied through CLI")
            data[key], sources[key] = value, "cli"
        for key in data:
            names = [key, *_ENV_ALIASES.get(key, ())]
            for name in names:
                if name in env:
                    default = defaults._defaults[key] if key in self._framework_keys else data[key]
                    data[key] = self._decode(key, env[name], default)
                    sources[key] = "environment"
                    break
        self._validate(data)
        # JSON-compatible containers become immutable too, without shared defaults.
        data = {k: _freeze(v) for k, v in data.items()}
        self._data, self.sources = MappingProxyType(data), MappingProxyType(sources)

    _decode = staticmethod(decode_environment_value)

    def getenv(self, name, default=None, cast=None):
        """Read one exact environment name from this Crawler's startup snapshot.

        Missing names return default; empty strings are explicit values. Infer a
        type from default, or pass cast=int/bool/float/str/list/dict/tuple.
        """
        if cast is not None and cast not in _ENV_TYPES:
            raise ConfigurationError("Unsupported environment value type")
        if name not in self._environ:
            return default
        return self._decode(name, self._environ[name], default, cast)

    @staticmethod
    def _validate(data):
        for key in ("LOG_SOURCE", "LOG_TOPIC"):
            value = data[key]
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ConfigurationError(f"{key} must be a nonempty string or None")
        integers = {"SEED_CONCURRENCY", "REQUEST_CONCURRENCY", "SEED_MAX_EXECUTIONS", "ITEM_BATCH_SIZE",
                    "REQUEST_BUFFER_LIMIT", "SEED_REQUEST_LIMIT", "ITEM_BUFFER_LIMIT", "DEDUP_TTL"}
        nonnegative_int = {"REQUEST_MAX_RETRIES", "ITEM_MAX_RETRIES"}
        numbers = {"REQUEST_TIMEOUT", "LEASE_TIMEOUT", "LEASE_RENEW_INTERVAL", "RECOVERY_SCAN_INTERVAL",
                   "ITEM_FLUSH_INTERVAL", "ITEM_RETRY_TIMEOUT", "SHUTDOWN_TIMEOUT", "POLL_INTERVAL", "METRICS_INTERVAL"}
        nonnegative = {"REQUEST_INTERVAL", "REQUEST_RETRY_BACKOFF", "REQUEST_RETRY_BACKOFF_MAX", "RETRY_AFTER_MAX",
                       "ITEM_RETRY_BACKOFF", "ITEM_RETRY_BACKOFF_MAX"}
        for key in integers | nonnegative_int:
            value = data[key]
            if type(value) is not int or value < (0 if key in nonnegative_int else 1):
                raise ConfigurationError(f"{key} must be an integer >= {0 if key in nonnegative_int else 1}")
        for key in numbers | nonnegative:
            value = data[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0 or (key in numbers and value == 0):
                raise ConfigurationError(f"{key} has an invalid duration")
        if data["LEASE_RENEW_INTERVAL"] >= data["LEASE_TIMEOUT"]:
            raise ConfigurationError("LEASE_RENEW_INTERVAL must be shorter than LEASE_TIMEOUT")
        if data["DEDUP_TTL"] < data["LEASE_TIMEOUT"] * data["SEED_MAX_EXECUTIONS"] + data["ITEM_RETRY_TIMEOUT"]:
            raise ConfigurationError("DEDUP_TTL must cover execution and storage recovery budgets")
        if data["ITEM_BATCH_SIZE"] > data["ITEM_BUFFER_LIMIT"]:
            raise ConfigurationError("ITEM_BATCH_SIZE cannot exceed ITEM_BUFFER_LIMIT")
        from spider_man.network.proxy_pool.proxy_provider import normalize_proxy
        try:
            normalize_proxy(data["DEFAULT_PROXY"])
        except ValueError:
            raise ConfigurationError("Invalid DEFAULT_PROXY") from None
        if data["LOG_LEVEL"] not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ConfigurationError("Invalid LOG_LEVEL")
        for key in ("LOG_JSON", "ALLOW_CONSOLE_ONLY"):
            if type(data[key]) is not bool:
                raise ConfigurationError(f"{key} must be boolean")
        for key in ("DOWNLOADER", "PROXY_PROVIDER"):
            if (key == "DOWNLOADER" and data[key] is None) or (data[key] is not None and (not isinstance(data[key], str) or "." not in data[key])):
                raise ConfigurationError(f"{key} must be a dotted class path")
        pipelines = data["ITEM_PIPELINES"]
        if not isinstance(pipelines, (list, tuple)) or not pipelines or any(not isinstance(x, str) or "." not in x for x in pipelines):
            raise ConfigurationError("ITEM_PIPELINES must be a nonempty list of class paths")
        statuses = data["RETRY_HTTP_STATUS_CODES"]
        if not isinstance(statuses, (list, tuple)) or any(type(x) is not int or not 400 <= x <= 599 for x in statuses):
            raise ConfigurationError("RETRY_HTTP_STATUS_CODES must be a list of HTTP error status codes")
        retry_exceptions = data["RETRY_EXCEPTIONS"]
        if not isinstance(retry_exceptions, (list, tuple)) or any(not isinstance(x, str) or "." not in x for x in retry_exceptions):
            raise ConfigurationError("RETRY_EXCEPTIONS must contain exception class paths")
        keys = data["MONGO_UPSERT_KEYS"]
        if keys is not None and (not isinstance(keys, (list, tuple)) or not keys or any(not isinstance(k, str) or not k for k in keys) or len(set(keys)) != len(keys)):
            raise ConfigurationError("MONGO_UPSERT_KEYS must be a nonempty list of distinct fields")

    def validate_connections(self):
        for key, schemes in (("REDIS_URL", ("redis", "rediss")), ("MONGO_URI", ("mongodb", "mongodb+srv"))):
            value = self[key]
            try:
                parts = urlsplit(value) if isinstance(value, str) else None
                valid = parts and parts.scheme in schemes and parts.hostname
            except ValueError:
                valid = False
            if not valid:
                raise ConfigurationError(f"{key} is required and must be a valid connection URL")
        value = self["MONGO_DATABASE"]
        if not isinstance(value, str) or not value or any(c in value for c in '/\\. "$\x00'):
            raise ConfigurationError("MONGO_DATABASE is required and must be a valid database name")

    def summary(self):
        # Platform variables can be marked secret under any business name. Keep
        # all business values out of automatic startup/check logs, not just names
        # that happen to contain PASSWORD or TOKEN.
        return {key: {"value": "[redacted]" if key not in self._framework_keys or key in ("REDIS_URL", "MONGO_URI", "DEFAULT_PROXY") else _plain(value),
                      "source": self.sources[key]} for key, value in self.items()}

    def with_cli(self, overrides):
        for key in overrides:
            if key not in _CLI_SETTINGS:
                raise ConfigurationError(f"Setting {key} cannot be supplied through CLI")
        effective = {key: value for key, value in overrides.items() if self.sources[key] != "environment"}
        updated = Settings(_plain(self._data), cli=effective, environ={})
        updated.sources = MappingProxyType({**self.sources, **{key: "cli" for key in effective}})
        updated._environ = self._environ
        return updated

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as exc:
            raise AttributeError(key) from exc


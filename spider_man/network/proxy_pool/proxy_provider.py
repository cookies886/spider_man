"""Small extension contract for task-specific proxy suppliers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any


Proxy = str | Mapping[str, str | None] | bool | None


@dataclass(frozen=True)
class ProxyContext(Mapping[str, Any]):
    """Context for one request attempt, available as attributes or a mapping."""

    project: str
    task: str
    seed_id: str | None
    attempt: int

    def __getitem__(self, name: str) -> Any:
        if name not in ("project", "task", "seed_id", "attempt"):
            raise KeyError(name)
        return getattr(self, name)

    def __iter__(self) -> Iterator[str]:
        return iter(("project", "task", "seed_id", "attempt"))

    def __len__(self) -> int:
        return 4


class ProxyProvider:
    """Subclass and configure its class path in ``PROXY_PROVIDER``.

    Providers are shared by the downloader's threads and should synchronize
    their own mutable state. Return a proxy URL, a Requests proxy mapping, or
    ``None``/``False`` for direct access. Feedback concerns transport and HTTP
    outcomes; a later business parsing error does not change this feedback.
    """

    def __init__(self, app: Any) -> None:
        self.app = app

    def get_proxy(self, context: ProxyContext) -> Proxy:
        return None

    def feedback(
        self,
        context: ProxyContext,
        proxy: Proxy,
        success: bool,
        error_type: str | None,
    ) -> None:
        pass

    def close(self) -> None:
        pass


def normalize_proxy(proxy: Proxy) -> dict[str, str | None]:
    """Convert proxy choices without consulting ambient environment settings."""

    if proxy is None or proxy is False:
        return {}
    if isinstance(proxy, str):
        if not proxy.strip():
            raise ValueError("Proxy URL cannot be empty")
        return {"http": proxy, "https": proxy}
    if isinstance(proxy, Mapping):
        if not all(
            isinstance(key, str) and (value is None or isinstance(value, str))
            for key, value in proxy.items()
        ):
            raise ValueError("Proxy mappings require string keys and URL values")
        return dict(proxy)
    raise ValueError("Proxy must be a URL, mapping, False, or None")

"""Small, predictable redis-py adapter; it does not patch global encoders."""

from typing import Any

import redis


class RedisDB:
    def __init__(self, url: str, **kwargs: Any) -> None:
        if not isinstance(url, str) or not url.strip():
            raise ValueError("REDIS_URL is required")
        if kwargs.get("decode_responses", True) is not True:
            raise ValueError("RedisDB requires decode_responses=True")
        kwargs["decode_responses"] = True
        kwargs.setdefault("socket_connect_timeout", 5)
        kwargs.setdefault("socket_timeout", 5)
        kwargs.setdefault("health_check_interval", 30)
        self.client = redis.Redis.from_url(url, **kwargs)

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> "RedisDB":
        return cls(url, **kwargs)

    def ping(self) -> bool:
        return bool(self.client.ping())

    def close(self) -> None:
        self.client.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def __repr__(self) -> str:
        # Redis URLs may contain passwords. Never display the URL here.
        return "RedisDB()"

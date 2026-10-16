"""Proxy supplier extension contract and normalization."""

from spider_man.network.proxy_pool.proxy_provider import (
    Proxy, ProxyContext, ProxyProvider, normalize_proxy,
)

__all__ = ["Proxy", "ProxyContext", "ProxyProvider", "normalize_proxy"]

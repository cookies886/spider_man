"""HTTP request values for a single download attempt."""
from __future__ import annotations

import math


class Request:
    def __init__(self, url, *, method="GET", params=None, headers=None, cookies=None,
                 data=None, json=None, timeout=None, proxies=None, seed=None, meta=None,
                 callback=None, max_retries=None, retryable=None, request_id=None,
                 allow_redirects=True, verify=True):
        from urllib.parse import urlsplit
        parsed = urlsplit(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("Request URL must be an absolute HTTP(S) URL")
        if not isinstance(method, str) or not method.isalpha():
            raise ValueError("Invalid HTTP method")
        if callback is not None and not isinstance(callback, str):
            raise ValueError("Request callback must be a registered string name")
        if request_id is not None and (not isinstance(request_id, str) or not request_id or len(request_id) > 256):
            raise ValueError("request_id must be a nonempty string of at most 256 characters")
        if max_retries is not None and (type(max_retries) is not int or max_retries < 0):
            raise ValueError("max_retries must be a nonnegative integer")
        if retryable is not None and type(retryable) is not bool:
            raise ValueError("retryable must be bool or None")
        if type(allow_redirects) is not bool:
            raise ValueError("allow_redirects must be boolean")
        if not isinstance(verify, (bool, str)) or isinstance(verify, str) and not verify:
            raise ValueError("verify must be boolean or a CA bundle path")
        if timeout is not None:
            values = timeout if isinstance(timeout, tuple) else (timeout,)
            if isinstance(timeout, tuple) and len(timeout) != 2:
                raise ValueError("timeout tuple must be (connect_seconds, read_seconds)")
            if any(isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or v <= 0 for v in values):
                raise ValueError("timeout must be positive seconds")
        self.url, self.method = url, method.upper()
        self.params, self.headers, self.cookies = params, dict(headers or {}), cookies
        self.data, self.json, self.timeout, self.proxies = data, json, timeout, proxies
        self.seed, self.meta, self.callback = seed, dict(meta or {}), callback
        self.max_retries, self.retryable = max_retries, retryable
        self.request_id = request_id
        self.allow_redirects, self.verify = allow_redirects, verify

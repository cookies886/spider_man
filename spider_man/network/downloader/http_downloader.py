"""HTTP transport with one attempt per call and isolated thread sessions."""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter

from spider_man.network.request import Request
from spider_man.network.response import Response
from spider_man.config import import_object
from spider_man.exceptions import DownloadError
from spider_man.network.proxy_pool.proxy_provider import ProxyContext, normalize_proxy


DEFAULT_RETRY_HTTP_STATUS_CODES = frozenset((408, 429, 500, 502, 503, 504))


def _safe_url(url: Any, *, hide_path: bool = True) -> str:
    """Keep an origin while hiding credentials and potentially signed URLs."""

    try:
        parsed = urlsplit(str(url))
        if parsed.scheme.lower() not in ("http", "https", "socks4", "socks5", "socks5h"):
            return "<redacted-url>"
        hostname = parsed.hostname
        if not hostname:
            return "<redacted-url>"
        host = f"[{hostname}]" if ":" in hostname else hostname
        port = f":{parsed.port}" if parsed.port is not None else ""
        path = "/<redacted>" if hide_path and parsed.path else ""
        query = "?<redacted>" if parsed.query else ""
        return f"{parsed.scheme.lower()}://{host}{port}{path}{query}"
    except (TypeError, ValueError):
        return "<redacted-url>"


def safe_request_summary(request: Request) -> dict[str, Any]:
    """Return a JSON-safe diagnostic summary without request secret values.

    Paths may contain access tokens just like query strings. Both are hidden;
    header, cookie, form and JSON values are never included in this summary.
    """

    proxy = request.proxies
    if proxy is False:
        proxy_summary: Any = "direct"
    elif proxy is None:
        proxy_summary = "task-default"
    elif isinstance(proxy, str):
        proxy_summary = _safe_url(proxy, hide_path=False)
    elif isinstance(proxy, Mapping):
        proxy_summary = {
            str(key): _safe_url(value, hide_path=False) if value else "direct"
            for key, value in proxy.items()
        }
    else:
        proxy_summary = "<invalid-proxy>"
    return {
        "request_id": request.request_id,
        "method": request.method,
        "url": _safe_url(request.url),
        "params": "<redacted>" if request.params is not None else None,
        "headers": {str(key): "<redacted>" for key in (request.headers or {})},
        "cookies": "<redacted>" if request.cookies else None,
        "data": "<redacted>" if request.data is not None else None,
        "json": "<redacted>" if request.json is not None else None,
        "proxies": proxy_summary,
    }


def parse_retry_after(value: str | None, maximum: float = 300.0) -> float | None:
    """Parse seconds or an HTTP date, ignoring invalid values and applying a cap."""

    if not value:
        return None
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            target = parsedate_to_datetime(value)
            if target.tzinfo is None:
                target = target.replace(tzinfo=timezone.utc)
            delay = max(0.0, (target - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None
    if not math.isfinite(delay) or delay < 0:
        return None
    return min(delay, max(0.0, float(maximum)))


class HTTPDownloader:
    """Reuse connections per thread; retry budgets belong to the Worker."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.settings = app.settings
        self._local = threading.local()
        self._lock = threading.Lock()
        self._sessions: list[requests.Session] = []
        self._closed = False
        self._provider = None
        provider_path = self.settings.get("PROXY_PROVIDER")
        if provider_path:
            provider_class = import_object(provider_path)
            self._provider = provider_class(app)
            if not callable(getattr(self._provider, "get_proxy", None)):
                raise DownloadError(
                    "Proxy provider must implement get_proxy(context)",
                    error_type="configuration", retryable=False,
                )
        self._retry_statuses = frozenset(self.settings.get(
            "RETRY_HTTP_STATUS_CODES", DEFAULT_RETRY_HTTP_STATUS_CODES
        ))

    def _session(self) -> requests.Session:
        with self._lock:
            if self._closed:
                raise DownloadError(
                    "Downloader is closed", error_type="configuration", retryable=False,
                )
            session = getattr(self._local, "session", None)
            if session is None:
                session = requests.Session()
                session.trust_env = False
                # Requests/urllib3 must not consume an additional retry budget.
                for scheme in ("http://", "https://"):
                    session.mount(scheme, HTTPAdapter(max_retries=0))
                self._local.session = session
                self._sessions.append(session)
            return session

    def _context(self, request: Request) -> ProxyContext:
        return ProxyContext(
            project=self.app.project,
            task=self.app.task,
            seed_id=getattr(request.seed, "seed_id", None),
            attempt=int(request.meta.get("_attempt", 1)),
        )

    def _feedback(self, context: ProxyContext, proxy: Any, success: bool, error_type: str | None) -> None:
        if self._provider is None:
            return
        callback = getattr(self._provider, "feedback", None)
        if callable(callback):
            try:
                callback(context, proxy, success, error_type)
            except Exception:
                # Observability must not replace a request's actual outcome.
                pass

    def download(self, request: Request) -> Response:
        context = self._context(request)
        proxy = request.proxies
        used_provider = proxy is None and self._provider is not None
        if proxy is None:
            if used_provider:
                try:
                    proxy = self._provider.get_proxy(context)
                except Exception:
                    self._feedback(context, None, False, "proxy")
                    raise DownloadError(
                        "Proxy provider could not supply a proxy", error_type="proxy", retryable=True,
                    ) from None
            else:
                proxy = self.settings.get("DEFAULT_PROXY")
        try:
            proxies = normalize_proxy(proxy)
        except ValueError:
            if used_provider:
                self._feedback(context, proxy, False, "configuration")
            raise DownloadError(
                "Invalid proxy configuration", error_type="configuration", retryable=False,
            ) from None

        session = self._session()
        session.cookies.clear()
        started = time.monotonic()
        try:
            response = session.request(
                method=request.method,
                url=request.url,
                params=request.params,
                headers=request.headers,
                cookies=request.cookies,
                data=request.data,
                json=request.json,
                timeout=request.timeout if request.timeout is not None else self.settings.get("REQUEST_TIMEOUT", 30),
                proxies=proxies,
                allow_redirects=request.allow_redirects,
                verify=request.verify,
            )
        except requests.exceptions.RequestException as error:
            error_type, retryable = _classify_transport_error(error)
            if used_provider:
                self._feedback(context, proxy, False, error_type)
            raise DownloadError(
                f"HTTP request failed ({error_type})", error_type=error_type, retryable=retryable,
            ) from None
        except (ValueError, TypeError, OSError):
            if used_provider:
                self._feedback(context, proxy, False, "configuration")
            raise DownloadError(
                "Invalid HTTP request parameters", error_type="configuration", retryable=False,
            ) from None
        finally:
            session.cookies.clear()

        elapsed = time.monotonic() - started
        if response.status_code >= 400:
            error_type = "rate_limit" if response.status_code == 429 else "http_status"
            retryable = response.status_code in self._retry_statuses
            retry_after = parse_retry_after(
                response.headers.get("Retry-After"), self.settings.get("RETRY_AFTER_MAX", 300)
            ) if retryable else None
            if used_provider:
                self._feedback(context, proxy, False, error_type)
            response.close()
            raise DownloadError(
                f"HTTP status {response.status_code}", error_type=error_type,
                retryable=retryable, retry_after=retry_after, status_code=response.status_code,
            )
        if used_provider:
            self._feedback(context, proxy, True, None)
        return Response(request=request, response=response, elapsed=elapsed)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            sessions, self._sessions = self._sessions, []
        for session in sessions:
            session.close()
        provider_close = getattr(self._provider, "close", None)
        if callable(provider_close):
            try:
                provider_close()
            except Exception:
                pass


def _classify_transport_error(error: requests.exceptions.RequestException) -> tuple[str, bool]:
    if isinstance(error, requests.exceptions.ProxyError):
        return "proxy", True
    if isinstance(error, requests.exceptions.SSLError):
        return "tls", False
    if isinstance(error, requests.exceptions.Timeout):
        return "timeout", True
    if isinstance(error, (requests.exceptions.InvalidURL, requests.exceptions.MissingSchema,
                          requests.exceptions.InvalidSchema, requests.exceptions.InvalidHeader,
                          requests.exceptions.InvalidJSONError, requests.exceptions.URLRequired)):
        return "configuration", False
    if isinstance(error, requests.exceptions.TooManyRedirects):
        return "redirect", False
    if isinstance(error, requests.exceptions.ConnectionError):
        return "connection", True
    return "network", True

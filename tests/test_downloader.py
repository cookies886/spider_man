from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from spider_man.base import Request, Seed
from spider_man.network.downloader.http_downloader import HTTPDownloader, parse_retry_after, safe_request_summary
from spider_man.exceptions import DownloadError
from spider_man.network.proxy_pool.proxy_provider import ProxyContext, ProxyProvider, normalize_proxy


@pytest.fixture
def http_server():
    seen = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            self._respond()

        def do_POST(self):
            self._respond()

        def _respond(self):
            parsed = urlsplit(self.path)
            body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
            seen.append((self.command, self.path))
            status = int(parsed.path.rsplit("/", 1)[1]) if parsed.path.startswith("/status/") else 200
            if parsed.path == "/redirect-cookie":
                status = 302
            if parsed.path == "/slow":
                time.sleep(0.08)
            response = json.dumps({
                "method": self.command, "params": parse_qs(parsed.query),
                "cookie": self.headers.get("Cookie"), "body": body,
                "custom_header": self.headers.get("X-Test"),
            }).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(response)))
            if parsed.path in ("/set-cookie", "/redirect-cookie"):
                self.send_header("Set-Cookie", "server_identity=private; Path=/")
            if parsed.path == "/redirect-cookie":
                self.send_header("Location", "/echo")
            if status == 429:
                self.send_header("Retry-After", "99999")
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", seen
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


@pytest.fixture
def app():
    return SimpleNamespace(project="site", task="posts", settings={"REQUEST_TIMEOUT": 2})


@pytest.fixture
def downloader(app):
    transport = HTTPDownloader(app)
    yield transport
    transport.close()


def test_real_get_forwards_query_headers_cookies_and_seed(http_server, downloader):
    origin, _ = http_server
    seed = Seed({"user_id": "42"})
    request = Request(origin + "/echo", params={"page": 2}, headers={"X-Test": "value"},
                      cookies={"identity": "one"}, seed=seed)
    response = downloader.download(request)
    assert response.seed is seed and response.request is request
    assert response.status_code == 200 and response.elapsed >= 0
    assert response.json() == {"method": "GET", "params": {"page": ["2"]},
                               "cookie": "identity=one", "body": "", "custom_header": "value"}
    assert response.headers["Content-Type"].startswith("application/json")


@pytest.mark.parametrize("body_args, expected", [
    ({"data": {"field": "value"}}, "field=value"),
    ({"json": {"field": "value"}}, '{"field": "value"}'),
])
def test_real_post_supports_form_and_json(http_server, downloader, body_args, expected):
    origin, _ = http_server
    result = downloader.download(Request(origin + "/echo", method="POST", **body_args))
    assert result.json()["method"] == "POST"
    assert result.json()["body"] == expected


def test_cookies_do_not_cross_requests_but_work_during_redirect(http_server, downloader):
    origin, _ = http_server
    assert downloader.download(Request(origin + "/redirect-cookie")).json()["cookie"] == "server_identity=private"
    assert downloader.download(Request(origin + "/echo")).json()["cookie"] is None
    downloader.download(Request(origin + "/set-cookie", cookies={"identity": "first"}))
    assert downloader.download(Request(origin + "/echo", cookies={"identity": "second"})).json()["cookie"] == "identity=second"


def test_explicit_redirect_control(http_server, downloader):
    origin, seen = http_server
    response = downloader.download(Request(origin + "/redirect-cookie", allow_redirects=False))
    assert response.status_code == 302
    assert len(seen) == 1


@pytest.mark.parametrize("status, retryable", [(408, True), (429, True), (500, True), (503, True),
                                               (400, False), (401, False), (404, False), (501, False)])
def test_http_errors_are_classified_without_internal_retries(http_server, downloader, status, retryable):
    origin, seen = http_server
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request(origin + f"/status/{status}", max_retries=8))
    assert exc.value.retryable is retryable
    assert exc.value.error_type == ("rate_limit" if status == 429 else "http_status")
    assert exc.value.retry_after == (300 if status == 429 else None)
    assert len(seen) == 1


def test_timeout_is_one_attempt(http_server, downloader):
    origin, seen = http_server
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request(origin + "/slow", timeout=0.01))
    assert exc.value.error_type == "timeout"
    assert exc.value.retryable is True
    assert len(seen) == 1


def test_ambient_proxy_is_ignored(http_server, downloader, monkeypatch):
    origin, _ = http_server
    for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(name, "http://127.0.0.1:1")
    for name in ("NO_PROXY", "no_proxy"):
        monkeypatch.delenv(name, raising=False)
    assert downloader.download(Request(origin + "/echo", proxies=False)).status_code == 200
    assert downloader.download(Request(origin + "/echo")).status_code == 200


def _response(status=200, headers=None):
    result = requests.Response()
    result.status_code = status
    result._content = b"{}"
    result._content_consumed = True
    result.headers.update(headers or {})
    return result


class RecordingProvider(ProxyProvider):
    def __init__(self, app):
        super().__init__(app)
        self.contexts = []
        self.feedbacks = []
        self.proxy = "http://proxy.example:8123"
        self.feedback_raises = False

    def get_proxy(self, context):
        self.contexts.append(context)
        return self.proxy

    def feedback(self, context, proxy, success, error_type):
        self.feedbacks.append((context, proxy, success, error_type))
        if self.feedback_raises:
            raise RuntimeError("metrics failed")


def test_proxy_provider_is_authoritative_and_gets_attempt_feedback(app, monkeypatch):
    app.settings.update(PROXY_PROVIDER="tests.Provider", DEFAULT_PROXY="http://default:1")
    monkeypatch.setattr("spider_man.network.downloader.http_downloader.import_object", lambda _: RecordingProvider)
    calls = []
    monkeypatch.setattr(requests.Session, "request", lambda self, **kw: calls.append(kw) or _response())
    downloader = HTTPDownloader(app)
    provider = downloader._provider
    seed = Seed({"user_id": "one"})
    request = Request("https://example.com/echo", seed=seed, meta={"_attempt": 3})
    downloader.download(request)
    assert calls[-1]["proxies"] == {"http": "http://proxy.example:8123", "https": "http://proxy.example:8123"}
    assert dict(provider.contexts[-1]) == {"project": "site", "task": "posts", "seed_id": seed.seed_id, "attempt": 3}
    assert provider.feedbacks[-1][2:] == (True, None)
    assert calls[-1]["verify"] is True
    provider.proxy = None
    downloader.download(request)
    assert calls[-1]["proxies"] == {}
    assert len(provider.contexts) == 2
    downloader.download(Request("https://example.com/echo", proxies=False))
    assert calls[-1]["proxies"] == {} and len(provider.contexts) == 2
    downloader.download(Request("https://example.com/echo", proxies="http://explicit:2", verify=False))
    assert calls[-1]["proxies"]["https"] == "http://explicit:2" and len(provider.contexts) == 2
    assert calls[-1]["verify"] is False
    downloader.close()


def test_default_proxy_and_timeout_are_forwarded(app, monkeypatch):
    app.settings.update(DEFAULT_PROXY={"https": "http://default:1"}, REQUEST_TIMEOUT=11)
    calls = []
    monkeypatch.setattr(requests.Session, "request", lambda self, **kw: calls.append(kw) or _response())
    downloader = HTTPDownloader(app)
    downloader.download(Request("https://example.com/echo"))
    assert calls[0]["proxies"] == {"https": "http://default:1"}
    assert calls[0]["timeout"] == 11
    downloader.close()


@pytest.mark.parametrize("transport_error, expected_type, retryable", [
    (requests.exceptions.ProxyError("secret"), "proxy", True),
    (requests.exceptions.ConnectionError("secret"), "connection", True),
    (requests.exceptions.SSLError("secret"), "tls", False),
    (requests.exceptions.InvalidHeader("secret"), "configuration", False),
    (requests.exceptions.InvalidJSONError("secret"), "configuration", False),
    (requests.exceptions.TooManyRedirects("secret"), "redirect", False),
])
def test_feedback_cannot_mask_transport_error(app, monkeypatch, transport_error, expected_type, retryable):
    app.settings["PROXY_PROVIDER"] = "tests.Provider"
    monkeypatch.setattr("spider_man.network.downloader.http_downloader.import_object", lambda _: RecordingProvider)

    def fail(self, **kwargs):
        raise transport_error

    monkeypatch.setattr(requests.Session, "request", fail)
    downloader = HTTPDownloader(app)
    downloader._provider.feedback_raises = True
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request("https://user:secret@example.com/token?key=secret"))
    assert exc.value.error_type == expected_type and exc.value.retryable is retryable
    assert "secret" not in str(exc.value) and exc.value.__suppress_context__
    assert downloader._provider.feedbacks[-1][2:] == (False, expected_type)
    downloader.close()


def test_feedback_cannot_mask_http_status_or_success(app, monkeypatch):
    app.settings["PROXY_PROVIDER"] = "tests.Provider"
    monkeypatch.setattr("spider_man.network.downloader.http_downloader.import_object", lambda _: RecordingProvider)
    downloader = HTTPDownloader(app)
    downloader._provider.feedback_raises = True
    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: _response(429, {"Retry-After": "12"}))
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request("https://example.com/echo"))
    assert exc.value.retry_after == 12
    assert downloader._provider.feedbacks[-1][2:] == (False, "rate_limit")
    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: _response())
    assert downloader.download(Request("https://example.com/echo")).status_code == 200
    downloader.close()


def test_invalid_ca_bundle_os_error_is_permanent_and_redacted(downloader, monkeypatch):
    def fail(self, **kwargs):
        raise OSError("Could not find private_ca_secret.pem")

    monkeypatch.setattr(requests.Session, "request", fail)
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request("https://example.com/echo", verify="private_ca_secret.pem"))
    assert exc.value.error_type == "configuration" and exc.value.retryable is False
    assert "private_ca_secret" not in str(exc.value)
    assert exc.value.__suppress_context__


def test_provider_failure_gets_feedback_and_preserves_error_type(app, monkeypatch):
    app.settings["PROXY_PROVIDER"] = "tests.Provider"
    monkeypatch.setattr("spider_man.network.downloader.http_downloader.import_object", lambda _: RecordingProvider)
    downloader = HTTPDownloader(app)

    def fail(context):
        raise RuntimeError("proxy_supplier_secret")

    downloader._provider.get_proxy = fail
    downloader._provider.feedback_raises = True
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request("https://example.com/echo"))
    assert exc.value.error_type == "proxy" and exc.value.retryable is True
    assert "proxy_supplier_secret" not in str(exc.value)
    assert downloader._provider.feedbacks[-1][1:] == (None, False, "proxy")
    downloader.close()


def test_retry_status_override(app, monkeypatch):
    app.settings["RETRY_HTTP_STATUS_CODES"] = [403]
    monkeypatch.setattr(requests.Session, "request", lambda *args, **kwargs: _response(403))
    downloader = HTTPDownloader(app)
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request("https://example.com/echo"))
    assert exc.value.retryable is True
    downloader.close()


def test_safe_summary_hides_all_secret_values():
    request = Request(
        "https://username:password@example.com/path_token?query_token=secret_value#fragment_token",
        params={"access_token": "param_secret"}, headers={"Authorization": "header_secret", "X-Api-Key": "api_secret"},
        cookies={"identity": "cookie_secret"}, data="body_secret", json={"field": "json_secret"},
        proxies="http://proxy_user:proxy_password@proxy.example:8123",
    )
    summary = json.dumps(safe_request_summary(request))
    for secret in ("username", "password", "path_token", "query_token", "secret_value", "fragment_token",
                   "param_secret", "header_secret", "api_secret", "cookie_secret", "body_secret", "json_secret",
                   "proxy_user", "proxy_password"):
        assert secret not in summary
    assert "example.com" in summary and "proxy.example:8123" in summary


def test_retry_after_date_and_invalid_values():
    target = datetime.now(timezone.utc) + timedelta(seconds=60)
    assert 58 <= parse_retry_after(format_datetime(target, usegmt=True)) <= 60
    past = datetime.now(timezone.utc) - timedelta(seconds=60)
    assert parse_retry_after(format_datetime(past, usegmt=True)) == 0
    assert parse_retry_after("999999", maximum=20) == 20
    for invalid in (None, "", "invalid", "-10", "nan", "inf"):
        assert parse_retry_after(invalid) is None


def test_threads_do_not_share_sessions_and_close_prevents_use(http_server, downloader):
    origin, _ = http_server
    barrier = threading.Barrier(2)

    def run(identity):
        barrier.wait(timeout=3)
        result = downloader.download(Request(origin + "/echo", cookies={"identity": identity}))
        return result.json()["cookie"], id(downloader._local.session)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, identity) for identity in ("a", "b")]
        results = [future.result(timeout=3) for future in futures]
    assert {result[0] for result in results} == {"identity=a", "identity=b"}
    assert len({result[1] for result in results}) == 2
    downloader.close()
    downloader.close()
    with pytest.raises(DownloadError) as exc:
        downloader.download(Request(origin + "/echo"))
    assert exc.value.retryable is False


def test_proxy_contract_validation():
    assert normalize_proxy(False) == {} and normalize_proxy(None) == {}
    assert normalize_proxy("http://example.com") == {"http": "http://example.com", "https": "http://example.com"}
    for invalid in (True, 3, "", {"http": 5}):
        with pytest.raises(ValueError):
            normalize_proxy(invalid)
    context = ProxyContext("site", "task", "seed", 2)
    assert context.attempt == context["attempt"] == 2
    with pytest.raises(KeyError):
        context["missing"]

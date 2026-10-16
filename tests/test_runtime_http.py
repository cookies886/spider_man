"""Worker request budgets against a real HTTP server and isolated real Redis.

Mongo is mocked here to keep retry and callback assertions independent of an
external Mongo service; Mongo persistence acceptance has its own coverage.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from types import SimpleNamespace

import mongomock
import pytest

from spider_man import Crawler
from spider_man.base import Item, Request, Seed
from spider_man.db.mongodb import MongoDB
from spider_man.config import Settings
from spider_man.network.downloader.http_downloader import HTTPDownloader
from spider_man.exceptions import ConfigurationError, DownloadError, RetryableError
from spider_man.core.crawler import Execution


@pytest.fixture
def runtime_http_server():
    calls = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.respond()

        def respond(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            calls.append((self.command, self.path))
            status = 503 if self.path == "/unavailable" else 200
            if self.path == "/child-retry" and sum(path == self.path for _, path in calls) <= 3:
                status = 503
            body = json.dumps({"items": [{"business_id": "one"}]}).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", calls
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


@pytest.fixture
def runtime_app(redis_client):
    mongo = MongoDB(client=mongomock.MongoClient(), db="runtime_http")
    app = Crawler("http_test", "posts", redis=redis_client, mongo=mongo, custom_settings={
        "REDIS_URL": "redis://localhost:6379/15",
        "MONGO_URI": "mongodb://localhost:27017",
        "MONGO_DATABASE": "runtime_http",
        "SEED_CONCURRENCY": 1,
        "REQUEST_CONCURRENCY": 1,
        "REQUEST_TIMEOUT": 1,
        "REQUEST_MAX_RETRIES": 2,
        "REQUEST_RETRY_BACKOFF": 0,
        "REQUEST_RETRY_BACKOFF_MAX": 0,
        "ITEM_BATCH_SIZE": 1,
        "ITEM_FLUSH_INTERVAL": 0.01,
        "POLL_INTERVAL": 0.01,
        "METRICS_INTERVAL": 0.1,
        "LOG_LEVEL": "CRITICAL",
    })

    @app.on_start
    def create_business_identity():
        mongo.create_index(app.collection_name, ["business_id"], unique=True)

    yield app
    app.close()
    mongo.close()


def _run(app):
    seed = Seed({"identity": "seed"})
    assert app.enqueue(seed)
    stats = app.start(until_idle=True, idle_timeout=0.02)
    return seed, stats["counters"]


def test_request_retry_exhaustion_finalizes_failure_without_rerunning_seed(runtime_app, runtime_http_server):
    app = runtime_app
    origin, calls = runtime_http_server
    request_calls = []

    @app.request
    def request(seed):
        request_calls.append(seed.seed_id)
        yield Request(origin + "/unavailable", seed=seed)

    @app.parse
    def parse(response):
        pytest.fail("HTTP errors must not reach parse")

    seed, counters = _run(app)
    assert len(calls) == 3 and len(request_calls) == 1
    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 1}
    failure = app.queue.failed(seed_id=seed.seed_id)[0]
    assert failure["execution_count"] == 1 and len(failure["executions"]) == 1
    assert failure["executions"][0]["request_attempts"] == 3
    assert failure["partial_summary"]["requests_completed"] == 0
    assert counters["request_attempt"] == 3 and counters["request_retry"] == 2
    assert counters["request_failure"] == 1 and counters["seed_failed"] == 1
    assert counters.get("seed_retried", 0) == 0


@pytest.mark.parametrize("retryable, expected_attempts", [(None, 1), (False, 1), (True, 3)])
def test_post_does_not_retry_without_explicit_business_authorization(runtime_app, runtime_http_server,
                                                                    retryable, expected_attempts):
    app = runtime_app
    origin, calls = runtime_http_server

    @app.request
    def request(seed):
        yield Request(origin + "/unavailable", method="POST", json={"read": "posts"},
                      seed=seed, retryable=retryable)

    @app.parse
    def parse(response):
        return None

    seed, counters = _run(app)
    assert len(calls) == expected_attempts
    assert all(call[0] == "POST" for call in calls)
    failure = app.queue.failed(seed_id=seed.seed_id)[0]
    assert failure["execution_count"] == 1
    assert failure["executions"][0]["request_attempts"] == expected_attempts
    assert counters["request_attempt"] == expected_attempts
    assert counters.get("request_retry", 0) == expected_attempts - 1


def test_parse_generator_can_request_retry_before_first_output(runtime_app, runtime_http_server):
    app = runtime_app
    origin, calls = runtime_http_server
    parse_attempts = []

    @app.request
    def request(seed):
        yield Request(origin + "/posts", seed=seed)

    @app.parse
    def parse(response):
        parse_attempts.append(response.request.meta["_attempt"])
        if len(parse_attempts) < 3:
            raise RetryableError("business body is not ready")
        yield Item({"business_id": "one", "attempt": len(parse_attempts)}, seed=response.seed)

    seed, counters = _run(app)
    assert parse_attempts == [1, 2, 3] and len(calls) == 3
    assert app.mongo.count(app.collection_name) == 1
    assert app.mongo.find(app.collection_name, {"business_id": "one"})[0]["attempt"] == 3
    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}
    execution = app.queue.executions(seed.seed_id)[0]
    assert execution["status"] == "succeeded" and execution["request_attempts"] == 3
    assert counters["request_retry"] == 2 and counters["request_success"] == 1
    assert counters["item_inserted"] == counters["seed_succeeded"] == 1


def test_parse_generator_retry_after_item_keeps_partial_write_and_same_request_id(runtime_app, runtime_http_server):
    app = runtime_app
    origin, calls = runtime_http_server
    parse_attempts = []
    request_callbacks = []

    @app.request
    def request(seed):
        request_callbacks.append(seed.seed_id)
        yield Request(origin + "/posts", seed=seed)

    @app.parse
    def parse(response):
        parse_attempts.append(response.request.meta["_attempt"])
        yield Item({"business_id": "one", "attempt": len(parse_attempts)}, seed=response.seed)
        if len(parse_attempts) == 1:
            raise RetryableError("retry after a confirmed partial page")

    seed, counters = _run(app)
    assert parse_attempts == [1, 2] and len(calls) == 2 and len(request_callbacks) == 1
    records = app.mongo.find(app.collection_name, {"business_id": "one"})
    assert len(records) == 1 and records[0]["attempt"] == 2
    assert app.queue.counts()["failed"] == 0
    execution = app.queue.executions(seed.seed_id)[0]
    assert execution["status"] == "succeeded" and execution["request_attempts"] == 2
    assert len(execution["requests"]) == 1
    assert counters["request_success"] == 1 and counters["request_retry"] == 1
    assert counters["item_inserted"] == 1 and counters["item_updated"] == 1
    assert counters["seed_succeeded"] == 1


@pytest.mark.parametrize("after_item", [False, True])
def test_yielded_none_is_invalid_output_and_never_counts_as_empty_success(runtime_app, runtime_http_server,
                                                                        after_item):
    app = runtime_app
    origin, calls = runtime_http_server

    @app.request
    def request(seed):
        yield Request(origin + "/posts", seed=seed)

    @app.parse
    def parse(response):
        if after_item:
            yield Item({"business_id": "one"}, seed=response.seed)
        yield None

    seed, counters = _run(app)
    assert len(calls) == 1
    assert app.queue.counts()["failed"] == 1
    failure = app.queue.failed(seed_id=seed.seed_id)[0]
    assert failure["error_type"] == "TypeError" and failure["stage"] == "parse"
    assert failure["partial_summary"]["items_confirmed"] == int(after_item)
    assert counters.get("request_success", 0) == 0
    assert counters.get("empty_result", 0) == 0
    assert counters["request_failure"] == 1 and counters["seed_failed"] == 1


def test_return_none_is_valid_empty_result(runtime_app, runtime_http_server):
    app = runtime_app
    origin, calls = runtime_http_server

    @app.request
    def request(seed):
        yield Request(origin + "/posts", seed=seed)

    @app.parse
    def parse(response):
        return None

    _, counters = _run(app)
    assert len(calls) == 1 and app.queue.counts()["failed"] == 0
    assert counters["empty_result"] == counters["request_success"] == counters["seed_succeeded"] == 1


@pytest.mark.parametrize("proxy", [True, 1, [], "", {"https": 1}, {1: "http://proxy.example"}])
def test_invalid_default_proxy_is_rejected_before_worker_starts(proxy):
    with pytest.raises(ConfigurationError, match="DEFAULT_PROXY"):
        Settings(custom_settings={"DEFAULT_PROXY": proxy}, environ={})


@pytest.mark.parametrize("timeout", [(), (1,), (1, 2, 3)])
def test_request_timeout_tuple_must_have_connect_and_read_values(timeout):
    with pytest.raises(ValueError, match="timeout"):
        Request("https://example.com", timeout=timeout)


def test_settings_snapshot_does_not_follow_mutation_of_input_containers():
    proxy = {"https": "http://proxy.example:8123"}
    statuses = [408, 429]
    settings = Settings(custom_settings={"DEFAULT_PROXY": proxy, "RETRY_HTTP_STATUS_CODES": statuses}, environ={})
    proxy["https"] = "http://changed.example:8123"
    statuses.append(503)
    assert settings["DEFAULT_PROXY"]["https"] == "http://proxy.example:8123"
    assert settings["RETRY_HTTP_STATUS_CODES"] == (408, 429)
    with pytest.raises(TypeError):
        settings["DEFAULT_PROXY"]["https"] = "http://changed.example:8123"
    assert json.loads(json.dumps(settings.summary()))["DEFAULT_PROXY"]["value"] == "[redacted]"


def test_retry_backoff_jitter_respects_configured_maximum(monkeypatch):
    app = Crawler("http_test", "backoff", custom_settings={
        "REQUEST_MAX_RETRIES": 2, "REQUEST_RETRY_BACKOFF": 0.5, "REQUEST_RETRY_BACKOFF_MAX": 0.5,
    })
    app.queue = SimpleNamespace(record_request=lambda *args: True, is_owned=lambda claim: True)

    @app.parse
    def parse(response):
        return None

    def fail(request):
        raise DownloadError("temporary network failure", retryable=True)

    app.downloader = SimpleNamespace(download=fail)
    waits = []

    class SimulatedLease:
        def is_set(self):
            return False

        def wait(self, seconds):
            waits.append(seconds)
            return False

    seed = Seed({"identity": "one"})
    context = Execution(SimpleNamespace(seed=seed), lost=SimulatedLease())
    monkeypatch.setattr("spider_man.core.crawler.random.uniform", lambda *args: 1.2)
    with pytest.raises(DownloadError):
        list(app._download_parse(Request("https://example.com", seed=seed, request_id="one"), context))
    assert max(waits) == 0.5
    assert waits.count(0.5) == 2
    assert app.metrics.snapshot()["counters"]["request_attempt"] == 3


def test_custom_downloader_programming_error_records_terminal_request_failure(runtime_app, monkeypatch):
    app = runtime_app

    class BrokenDownloader:
        def __init__(self, app):
            pass

        def download(self, request):
            raise RuntimeError("custom downloader failed")

        def close(self):
            pass

    monkeypatch.setattr("spider_man.core.crawler.import_object", lambda path: BrokenDownloader)

    @app.request
    def request(seed):
        yield Request("https://example.com", seed=seed)

    @app.parse
    def parse(response):
        pytest.fail("Programming error should not reach parse")

    seed, counters = _run(app)
    failure = app.queue.failed(seed_id=seed.seed_id)[0]
    assert failure["stage"] == "download" and failure["error_type"] == "RuntimeError"
    assert failure["executions"][0]["request_attempts"] == 1
    assert counters["request_attempt"] == counters["request_failure"] == counters["seed_failed"] == 1
    assert counters.get("request_retry", 0) == 0


def test_configured_custom_downloader_exception_retries_then_succeeds(runtime_app, runtime_http_server,
                                                                     monkeypatch):
    app = runtime_app
    origin, network_calls = runtime_http_server
    app.settings = Settings(custom_settings={**dict(app.settings), "RETRY_EXCEPTIONS": ["builtins.OSError"]},
                            environ={})
    attempts = []

    class RecoveringDownloader:
        def __init__(self, app):
            self.http = HTTPDownloader(app)

        def download(self, request):
            attempts.append(request.meta["_attempt"])
            if len(attempts) == 1:
                raise OSError("temporary custom transport failure")
            return self.http.download(request)

        def close(self):
            self.http.close()

    from spider_man.core import crawler
    original_import = crawler.import_object
    downloader_path = app.settings["DOWNLOADER"]
    monkeypatch.setattr(crawler, "import_object", lambda path: RecoveringDownloader
                        if path == downloader_path else original_import(path))

    @app.request
    def request(seed):
        yield Request(origin + "/posts", seed=seed)

    @app.parse
    def parse(response):
        yield Item(response.json()["items"][0], seed=response.seed)

    seed, counters = _run(app)
    assert attempts == [1, 2] and len(network_calls) == 1
    assert counters["request_attempt"] == 2 and counters["request_retry"] == 1
    assert counters["request_success"] == counters["item_inserted"] == counters["seed_succeeded"] == 1
    assert counters.get("request_failure", 0) == 0
    assert app.queue.executions(seed.seed_id)[0]["request_attempts"] == 2
    assert app.queue.counts()["failed"] == 0


def test_retry_exception_class_must_be_exception_before_hooks_or_claim(runtime_app):
    app = runtime_app
    app.settings = Settings(custom_settings={**dict(app.settings), "RETRY_EXCEPTIONS": ["builtins.str"]},
                            environ={})
    hooks = []

    @app.on_start
    def hook():
        hooks.append("called")

    @app.request
    def request(seed):
        yield Request("https://example.com", seed=seed)

    @app.parse
    def parse(response):
        return None

    seed = Seed({"identity": "pending"})
    app.enqueue(seed)
    with pytest.raises(ConfigurationError, match="Exception"):
        app.start(until_idle=True)
    assert hooks == []
    assert app.queue.counts() == {"todo": 1, "processing": 0, "retry": 0, "failed": 0}
    assert app.queue.executions(seed.seed_id) == []


def test_nested_request_identity_and_budget_stay_stable_through_parent_and_seed_retries(
        runtime_app, runtime_http_server, monkeypatch):
    app = runtime_app
    origin, calls = runtime_http_server
    app.settings = Settings(custom_settings={**dict(app.settings), "REQUEST_MAX_RETRIES": 3,
                            "LEASE_TIMEOUT": 0.12, "LEASE_RENEW_INTERVAL": 0.03,
                            "RECOVERY_SCAN_INTERVAL": 0.01}, environ={})
    root_requests = {1: [], 2: []}
    observed = {1: [], 2: []}

    @app.request
    def request(seed):
        parent = Request(origin + "/parent", seed=seed)
        root_requests[seed.execution_count].append(("parent", parent))
        yield parent
        tail = Request(origin + "/tail", seed=seed, callback="tail")
        root_requests[seed.execution_count].append(("tail", tail))
        yield tail

    @app.parse
    def parent(response):
        execution = response.seed.execution_count
        observed[execution].append(("parent", response.request.request_id, response.request.meta["_attempt"]))
        yield Request(origin + "/child", seed=response.seed, callback="child")
        if response.request.meta["_attempt"] < 4:
            raise RetryableError("parent body needs another attempt after its child")

    @app.parse(name="child")
    def child(response):
        observed[response.seed.execution_count].append(
            ("child", response.request.request_id, response.request.meta["_attempt"]))
        return None

    @app.parse(name="tail")
    def tail(response):
        observed[response.seed.execution_count].append(
            ("tail", response.request.request_id, response.request.meta["_attempt"]))
        return None

    seed = Seed({"identity": "nested"})
    assert app.enqueue(seed)
    original_ack = app.queue.ack
    refused = []

    def lose_first_ack(claim):
        if not refused:
            refused.append(claim.token)
            return False
        return original_ack(claim)

    monkeypatch.setattr(app.queue, "ack", lose_first_ack)
    app.start(max_seeds=1)
    assert app.queue.counts()["processing"] == 1
    assert app.queue.executions(seed.seed_id)[0]["request_attempts"] == 9
    # Simulate losing the worker after its outputs were confirmed but before
    # ACK. Automatic lease recovery must preserve Seed identity and root order.
    time.sleep(app.settings["LEASE_TIMEOUT"] + 0.04)
    app.start(until_idle=True, idle_timeout=0.02)

    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}
    histories = app.queue.executions(seed.seed_id)
    assert [entry["status"] for entry in histories] == ["recovered", "succeeded"]
    assert [entry["request_attempts"] for entry in histories] == [9, 9]
    assert calls == [("GET", path) for path in (["/parent", "/child"] * 4 + ["/tail"]) * 2]
    for execution in (1, 2):
        assert [role for role, _ in root_requests[execution]] == ["parent", "tail"]
        child_ids = {request_id for role, request_id, _ in observed[execution] if role == "child"}
        assert len(child_ids) == 1
        child_id = child_ids.pop()
        assert [attempt for role, _, attempt in observed[execution] if role == "child"] == [1, 2, 3, 4]
        assert len(histories[execution - 1]["requests"]) == 3
        assert histories[execution - 1]["requests"][child_id]["attempt_count"] == 4
    assert {role: request.request_id for role, request in root_requests[1]} == {
        role: request.request_id for role, request in root_requests[2]
    }
    assert {request_id for role, request_id, _ in observed[1] if role == "child"} == {
        request_id for role, request_id, _ in observed[2] if role == "child"
    }


def test_nested_request_budget_cannot_reset_after_parent_retries(runtime_app, runtime_http_server):
    app = runtime_app
    origin, calls = runtime_http_server
    app.settings = Settings(custom_settings={**dict(app.settings), "REQUEST_MAX_RETRIES": 3}, environ={})
    parents = []
    children = []

    @app.request
    def request(seed):
        yield Request(origin + "/parent", seed=seed)

    @app.parse
    def parent(response):
        parents.append((response.request.request_id, response.request.meta["_attempt"]))
        yield Request(origin + "/child-retry", seed=response.seed, callback="child")
        if response.request.meta["_attempt"] == 1:
            raise RetryableError("retry parent after child used its full budget")

    @app.parse(name="child")
    def child(response):
        children.append((response.request.request_id, response.request.meta["_attempt"]))
        return None

    seed, counters = _run(app)
    assert [attempt for _, attempt in parents] == [1, 2]
    assert len(children) == 1 and children[0][1] == 4
    assert sum(path == "/child-retry" for _, path in calls) == 4
    assert len(calls) == 6 and counters["request_attempt"] == 6
    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 1}
    failure = app.queue.failed(seed_id=seed.seed_id)[0]
    assert failure["execution_count"] == 1 and len(failure["executions"]) == 1
    assert "budget" in failure["error"]["message"].lower()
    history = failure["executions"][0]
    assert history["request_attempts"] == 6 and len(history["requests"]) == 2
    assert history["requests"][children[0][0]]["attempt_count"] == 4
    assert history["requests"][parents[0][0]]["attempt_count"] == 2

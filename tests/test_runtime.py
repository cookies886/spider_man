from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time

import mongomock
import pytest

from spider_man import Item, Crawler, Request, Seed
from spider_man.db.mongodb import MongoDB
from spider_man.exceptions import ConfigurationError
from spider_man.pipelines import MongoPipeline
from spider_man.core.queue import RedisQueue


@pytest.fixture
def local_http():
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            page = int(self.path.split("page=")[-1])
            body = json.dumps({"posts": [{"id": page, "title": f"page {page}"}],
                               "next_page": page + 1 if page < 3 else None}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/posts"
    server.shutdown()
    thread.join()
    server.server_close()


def make_app(redis_client, mongo, local_http, **overrides):
    settings = {"REDIS_URL": "redis://localhost:6379/0", "MONGO_URI": "mongodb://localhost:27017",
                "MONGO_DATABASE": "test", "ITEM_FLUSH_INTERVAL": 0.02, "POLL_INTERVAL": 0.01,
                "METRICS_INTERVAL": 60, "LOG_LEVEL": "ERROR", **overrides}
    app = Crawler("bilibili", "user_post", redis=redis_client, mongo=mongo, custom_settings=settings)
    @app.request
    def request(seed):
        yield Request(local_http, params={"page": seed.page or 1}, seed=seed)
    @app.parse
    def parse(response):
        body = response.json()
        for post in body["posts"]:
            yield Item(post, seed=response.seed)
            # Direct enqueue follows the same stable child identity protocol.
            app.enqueue(Seed({"id": post["id"]}), task="video_detail")
        if body["next_page"]:
            yield Seed({"page": body["next_page"]})
    return app


@pytest.mark.parametrize("unique", [True, False])
def test_full_seed_request_parse_pipeline_pagination(redis_client, local_http, unique):
    mongo = MongoDB(client=mongomock.MongoClient(), db="test")
    app = make_app(redis_client, mongo, local_http)
    if unique:
        @app.on_start
        def indexes():
            app.mongo.create_index(app.collection_name, ["id"], unique=True)
    app.start_seeds([{"page": 1}, {"page": 1}])
    outcome = app.start(until_idle=True, idle_timeout=0.05)
    assert outcome["counters"]["seed_succeeded"] == 6
    assert outcome["counters"]["request_success"] == 6
    assert mongo.count(app.collection_name) == (3 if unique else 6)
    target = RedisQueue(redis_client, "bilibili", "video_detail")
    assert target.counts()["todo"] == 6
    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}


def test_index_error_stops_before_claim(redis_client, local_http):
    mongo = MongoDB(client=mongomock.MongoClient(), db="test")
    app = make_app(redis_client, mongo, local_http, MONGO_UPSERT_KEYS=["id"])
    seed = Seed({"page": 1})
    app.enqueue(seed)
    with pytest.raises(ConfigurationError, match="unique index"):
        app.start(until_idle=True)
    assert app.queue.counts()["todo"] == 1
    assert app.queue.executions(seed.seed_id) == []


def test_check_does_not_run_start_hook_or_consume(redis_client, local_http):
    mongo = MongoDB(client=mongomock.MongoClient(), db="test")
    app = make_app(redis_client, mongo, local_http, MONGO_UPSERT_KEYS=["id"])
    @app.on_start
    def hook():
        raise AssertionError("check must not execute startup hook")
    app.enqueue(Seed({"page": 1}))
    result = app.check()
    assert not result["ready"] and result["startup_hooks_pending"] == 1
    assert app.queue.counts()["todo"] == 1
    assert mongo.get_index(app.collection_name) == {}


def test_mongo_failure_does_not_ack_and_preserves_partial(redis_client, local_http, monkeypatch):
    mongo = MongoDB(client=mongomock.MongoClient(), db="test")
    app = make_app(redis_client, mongo, local_http)
    calls = []
    original = MongoPipeline.process_item
    def fail_second(self, item):
        calls.append(item)
        if item.data["id"] == 2:
            raise ValueError("invalid business Item")
        return original(self, item)
    monkeypatch.setattr(MongoPipeline, "process_item", fail_second)
    app.start_seeds([{"page": 1}])
    app.start(until_idle=True, idle_timeout=0.05)
    assert mongo.count(app.collection_name) == 1
    failures = app.queue.failed()
    assert len(failures) == 1 and failures[0]["stage"] == "storage"
    assert failures[0]["error_type"] == "ValueError"
    assert app.metrics.snapshot()["counters"]["seed_succeeded"] == 1


def test_complete_real_mongo_flow(redis_client, real_mongo, local_http):
    app = make_app(redis_client, real_mongo, local_http)
    @app.on_start
    def indexes():
        app.mongo.create_index(app.collection_name, ["id"], unique=True)
    app.start_seeds([{"page": 1}, {"page": 1}])
    app.start(until_idle=True, idle_timeout=0.05)
    assert real_mongo.count(app.collection_name) == 3
    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}

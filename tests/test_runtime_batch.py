"""Batch confirmation barriers, partial failures, replay, and bounded stop."""

from concurrent.futures import Future
from dataclasses import dataclass, field
import threading
import time

import mongomock
import pytest

from spider_man import Item, Crawler, Seed
from spider_man.db.mongodb import MongoDB
from spider_man.core.crawler import Execution
from spider_man.pipelines import PipelineManager, PipelineResult
from spider_man.core.queue import RedisQueue


@dataclass
class Control:
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    batches: list = field(default_factory=list)
    failures: set = field(default_factory=set)


class ControlledBatch:
    required = True
    persistent = True

    def __init__(self, app):
        self.control = app._batch_control

    def open(self):
        pass

    def process_batch(self, items):
        self.control.batches.append([item.data["id"] for item in items])
        self.control.entered.set()
        if not self.control.release.wait(3):
            raise TimeoutError("Test did not release the batch")
        return [ValueError("Invalid test Item") if item.data["id"] in self.control.failures
                else PipelineResult("inserted") for item in items]

    def flush(self):
        pass

    def close(self):
        pass


def make_app(redis_client, control, **settings):
    app = Crawler("batch", "parent", redis=redis_client,
                   mongo=MongoDB(client=mongomock.MongoClient(), db="isolated_batch_test"),
                   custom_settings={
                       "REDIS_URL": "redis://localhost:1", "MONGO_URI": "mongodb://localhost:1",
                       "MONGO_DATABASE": "isolated_batch_test", "SEED_CONCURRENCY": 1,
                       "ITEM_BATCH_SIZE": 4, "ITEM_BUFFER_LIMIT": 8, "ITEM_FLUSH_INTERVAL": 0.01,
                       "ITEM_PIPELINES": [__name__ + ".ControlledBatch"],
                       "POLL_INTERVAL": 0.01, "SHUTDOWN_TIMEOUT": 1.0,
                       "LOG_LEVEL": "CRITICAL", **settings,
                   })
    app._batch_control = control

    @app.parse
    def parse(response):
        return None

    return app


def background(app):
    result = Future()

    def run():
        try:
            result.set_result(app.start(until_idle=True, idle_timeout=0.01))
        except BaseException as error:
            result.set_exception(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return result, thread


def cleanup(app, control, thread):
    control.release.set()
    app.stop()
    thread.join(timeout=3)
    assert not thread.is_alive(), "Crawler did not stop in the bounded test interval"


def test_seed_ack_waits_for_all_batch_confirmations(redis_client):
    control = Control()
    app = make_app(redis_client, control)

    @app.request
    def request(seed):
        for number in range(4):
            yield Item({"id": number}, seed=seed)

    app.enqueue(Seed({"id": "parent"}))
    result, thread = background(app)
    try:
        assert control.entered.wait(2)
        assert control.batches == [[0, 1, 2, 3]]
        assert app.queue.counts()["processing"] == 1
        assert app.metrics.snapshot()["counters"].get("seed_succeeded", 0) == 0
        control.release.set()
        counters = result.result(timeout=2)["counters"]
        assert counters["seed_succeeded"] == 1
        assert counters["item_inserted"] == 4
        assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}
    finally:
        cleanup(app, control, thread)


def test_direct_child_enqueue_waits_for_prior_storage(redis_client):
    control = Control()
    app = make_app(redis_client, control)
    called, finished = threading.Event(), threading.Event()

    @app.request
    def request(seed):
        yield Item({"id": 1}, seed=seed)
        yield Item({"id": 2}, seed=seed)
        called.set()
        app.enqueue(Seed({"id": "child"}), task="children")
        finished.set()

    app.enqueue(Seed({"id": "parent"}))
    child_queue = RedisQueue(redis_client, "batch", "children")
    result, thread = background(app)
    try:
        assert called.wait(2) and control.entered.wait(2)
        assert not finished.is_set()
        assert child_queue.counts()["todo"] == 0
        assert app.queue.counts()["processing"] == 1
        control.release.set()
        assert result.result(timeout=2)["counters"]["seed_succeeded"] == 1
        assert finished.is_set()
        assert child_queue.counts()["todo"] == 1
    finally:
        cleanup(app, control, thread)


def test_partial_batch_failures_retain_confirmed_item_count(redis_client):
    control = Control(failures={1, 3})
    control.release.set()
    app = make_app(redis_client, control)

    @app.request
    def request(seed):
        for number in range(4):
            yield Item({"id": number}, seed=seed)
        yield Seed({"id": "must-not-dispatch"}, task="children")

    app.enqueue(Seed({"id": "parent"}))
    result, thread = background(app)
    try:
        counters = result.result(timeout=2)["counters"]
        assert counters.get("seed_succeeded", 0) == 0
        failure = app.queue.failed()[0]
        assert failure["stage"] == "storage"
        assert failure["partial_summary"]["items_confirmed"] == 2
        assert failure["partial_summary"]["children_enqueued"] == 0
        assert RedisQueue(redis_client, "batch", "children").counts()["todo"] == 0
    finally:
        cleanup(app, control, thread)


def test_storage_failure_before_yielded_child_records_storage_stage(redis_client):
    control = Control(failures={1})
    control.release.set()
    app = make_app(redis_client, control)

    @app.request
    def request(seed):
        yield Item({"id": 1}, seed=seed)
        yield Seed({"id": "must-not-dispatch"}, task="children")

    app.enqueue(Seed({"id": "parent"}))
    result, thread = background(app)
    try:
        result.result(timeout=2)
        assert app.queue.failed()[0]["stage"] == "storage"
        assert RedisQueue(redis_client, "batch", "children").counts()["todo"] == 0
    finally:
        cleanup(app, control, thread)


def test_actual_replay_reuses_child_dispatched_before_parent_failure(redis_client):
    control = Control()
    control.release.set()
    app = make_app(redis_client, control)

    @app.request
    def request(seed):
        yield Item({"id": 1}, seed=seed)
        app.enqueue(Seed({"id": "child"}), task="children")
        if not seed.metadata.get("replay_of"):
            raise ValueError("Parent failed after confirmed outputs")

    parent = Seed({"id": "parent"})
    app.enqueue(parent)
    result, thread = background(app)
    try:
        result.result(timeout=2)
    finally:
        cleanup(app, control, thread)
    child_queue = RedisQueue(redis_client, "batch", "children")
    original_child = redis_client.zrange(child_queue.todo_key, 0, -1)
    assert len(original_child) == 1
    failure = app.queue.failed(seed_id=parent.seed_id)[0]
    assert failure["partial_summary"]["items_confirmed"] == 1
    assert failure["partial_summary"]["children_enqueued"] == 1
    replay = app.queue.replay(parent.seed_id)
    assert replay.metadata["replay_root"] == parent.seed_id
    result, thread = background(app)
    try:
        counters = result.result(timeout=2)["counters"]
        assert counters["seed_succeeded"] == 1
        assert counters["seed_duplicate"] == 1
        assert redis_client.zrange(child_queue.todo_key, 0, -1) == original_child
        assert len(app.queue.failed()) == 1
    finally:
        cleanup(app, control, thread)


def test_stop_returns_on_deadline_and_retains_seed_with_pending_batch(redis_client):
    control = Control()
    app = make_app(redis_client, control, SHUTDOWN_TIMEOUT=0.05,
                   LEASE_TIMEOUT=0.5, LEASE_RENEW_INTERVAL=0.1)

    @app.request
    def request(seed):
        yield Item({"id": 1}, seed=seed)
        raise ValueError("Parser failed while a prior write is pending")

    app.enqueue(Seed({"id": "parent"}))
    result, thread = background(app)
    try:
        assert control.entered.wait(2)
        before = time.monotonic()
        app.stop()
        result.result(timeout=0.5)
        assert time.monotonic() - before < 0.5
        time.sleep(0.06)
        assert app.queue.counts()["processing"] == 1
        assert app.queue.counts()["failed"] == 0
        assert app.metrics.snapshot()["counters"].get("seed_succeeded", 0) == 0
    finally:
        cleanup(app, control, thread)


def test_lease_expiring_while_child_waits_storage_blocks_dispatch(redis_client):
    # Run the execution without a maintainer to model missed renewals. The
    # storage call starts while owned, then confirms after its lease expires.
    control = Control()
    app = make_app(redis_client, control, LEASE_TIMEOUT=0.08, LEASE_RENEW_INTERVAL=0.02)

    @app.request
    def request(seed):
        yield Item({"id": 1}, seed=seed)
        app.enqueue(Seed({"id": "stale-child"}), task="children")

    app.enqueue(Seed({"id": "parent"}))
    context = Execution(app.queue.claim("missed-renewal"))
    app.pipeline = PipelineManager(app)
    app.pipeline.open()
    thread = threading.Thread(target=app._execute, args=(context,), daemon=True)
    thread.start()
    try:
        assert control.entered.wait(2)
        time.sleep(0.1)
        control.release.set()
        thread.join(timeout=1)
        assert not thread.is_alive()
        assert context.lost.is_set()
        assert RedisQueue(redis_client, "batch", "children").counts()["todo"] == 0
        assert app.queue.counts()["processing"] == 1
        assert app.queue.counts()["failed"] == 0
        assert app.queue.recover()["reclaimed"] == 1
    finally:
        control.release.set()
        thread.join(timeout=1)
        app.close()

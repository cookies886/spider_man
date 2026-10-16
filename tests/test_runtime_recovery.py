"""Process-level recovery and stale-output tests using isolated real Redis.

The confirmation Pipeline here deliberately writes only test markers to Redis;
it does not establish MongoDB or multi-host production acceptance.
"""

import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest

from spider_man.base import Seed
from spider_man.core.queue import RedisQueue


_WORKER = r"""
import sys
import time
from spider_man import Crawler
from spider_man.base import Item, Seed
from spider_man.db.redisdb import RedisDB
from spider_man.pipelines import PipelineResult

socket_path, worker, behavior = sys.argv[1:]

class TestMongo:
    def ping(self):
        return True

class MarkerPipeline:
    required = True
    persistent = True
    def __init__(self, app):
        self.app = app
    def open(self):
        pass
    def process_item(self, item):
        self.app.redis.rpush("runtime:confirmed", str(item.seed.execution_count))
        return PipelineResult("inserted")
    def flush(self):
        pass
    def close(self):
        pass

db = RedisDB("unix://" + socket_path)
app = Crawler("runtime", "recovery", redis=db, mongo=TestMongo(), custom_settings={
    "REDIS_URL": "redis://127.0.0.1:1", "MONGO_URI": "mongodb://127.0.0.1:1",
    "MONGO_DATABASE": "isolated_test", "SEED_CONCURRENCY": 1,
    "LEASE_TIMEOUT": 0.4, "LEASE_RENEW_INTERVAL": 0.1,
    "RECOVERY_SCAN_INTERVAL": 0.03, "SHUTDOWN_TIMEOUT": 0.05,
    "POLL_INTERVAL": 0.01, "ITEM_BATCH_SIZE": 1, "ITEM_FLUSH_INTERVAL": 0.01,
    "ITEM_PIPELINES": ["__main__.MarkerPipeline"], "LOG_LEVEL": "CRITICAL",
})

@app.on_start
def ready():
    db.set("runtime:ready:" + worker, "yes")

@app.request
def request(seed):
    db.set("runtime:claimed:" + worker, seed.execution_count)
    if behavior == "blocked":
        time.sleep(30)
    elif behavior in ("late-item", "late-child"):
        time.sleep(0.3)
    if behavior == "late-child":
        yield Seed({"child": True}, task="children")
    else:
        yield Item({"id": "confirmed"}, seed=seed)

@app.parse
def parse(result):
    return None

app.start(max_seeds=1)
db.set("runtime:returned:" + worker, "yes")
if behavior.startswith("late-"):
    # Give the formerly blocked callback time to resume after shutdown grace.
    time.sleep(0.45)
db.close()
"""


def _start(socket_path, name, behavior):
    return subprocess.Popen(
        [sys.executable, "-c", _WORKER, socket_path, name, behavior],
        cwd=Path(__file__).resolve().parents[1],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def _wait_key(client, key, process, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = client.get(key)
        if value is not None:
            return value
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(f"Worker exited before {key}: {stdout}\n{stderr}")
        time.sleep(0.01)
    pytest.fail(f"Worker did not produce {key}")


def _cleanup(process):
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=5)


def test_second_process_recovers_a_killed_worker(redis_client, redis_socket_shared):
    q = RedisQueue(redis_client, "runtime", "recovery", lease_timeout=0.4)
    seed = Seed({"id": "crash"})
    q.enqueue(seed)
    first = _start(redis_socket_shared, "first", "blocked")
    second = None
    try:
        assert _wait_key(redis_client, "runtime:claimed:first", first) == "1"
        second = _start(redis_socket_shared, "second", "complete")
        _wait_key(redis_client, "runtime:ready:second", second)
        time.sleep(0.1)
        assert redis_client.get("runtime:claimed:second") is None
        assert q.counts()["processing"] == 1
        first.kill()
        first.wait(timeout=5)
        assert _wait_key(redis_client, "runtime:claimed:second", second) == "2"
        stdout, stderr = second.communicate(timeout=5)
        assert second.returncode == 0, stdout + stderr
        assert redis_client.lrange("runtime:confirmed", 0, -1) == ["2"]
        assert q.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}
        assert [record["status"] for record in q.executions(seed.seed_id)] == ["recovered", "succeeded"]
    finally:
        _cleanup(first)
        if second is not None:
            _cleanup(second)


@pytest.mark.parametrize("behavior", ["late-item", "late-child"])
def test_callback_resuming_after_shutdown_cannot_emit_output(redis_client, redis_socket_shared, behavior):
    q = RedisQueue(redis_client, "runtime", "recovery", lease_timeout=0.4)
    q.enqueue(Seed({"id": "grace"}))
    worker = _start(redis_socket_shared, "late", behavior)
    try:
        _wait_key(redis_client, "runtime:claimed:late", worker)
        os.kill(worker.pid, signal.SIGTERM)
        _wait_key(redis_client, "runtime:returned:late", worker)
        stdout, stderr = worker.communicate(timeout=5)
        assert worker.returncode == 0, stdout + stderr
        assert redis_client.lrange("runtime:confirmed", 0, -1) == []
        children = RedisQueue(redis_client, "runtime", "children")
        assert children.counts()["todo"] == 0
        assert q.counts()["processing"] == 1
        assert q.recover()["reclaimed"] == 1
    finally:
        _cleanup(worker)

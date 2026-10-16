"""Real Redis tests for the atomic lease and failure protocols."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

import pytest
import redis

from spider_man.base import Seed
from spider_man.db.redisdb import RedisDB
from spider_man.core.queue import RedisQueue


@pytest.fixture(scope="module")
def redis_socket():
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server is required for queue integration tests")
    with tempfile.TemporaryDirectory(prefix="sm-redis-", dir="/tmp") as directory:
        socket_path = str(Path(directory) / "redis.sock")
        process = subprocess.Popen(
            [binary, "--port", "0", "--unixsocket", socket_path,
             "--save", "", "--appendonly", "no", "--dir", directory],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        client = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        try:
            for _ in range(200):
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    if process.poll() is not None:
                        pytest.fail("isolated redis-server exited during startup")
                    time.sleep(0.01)
            else:
                pytest.fail("isolated redis-server did not start")
            yield socket_path
        finally:
            client.close()
            process.terminate()
            process.wait(timeout=10)


@pytest.fixture
def client(redis_socket):
    instance = redis.Redis(unix_socket_path=redis_socket, decode_responses=True)
    instance.flushdb()
    yield instance
    instance.close()


def queue(client, **options):
    return RedisQueue(client, "bilibili", "user_post", **options)


def test_priority_identity_and_ack(client):
    q = queue(client)
    high, low = Seed({"page": 1}, priority=1), Seed({"page": 2}, priority=100)
    assert q.todo_key == "{bilibili:user_post}:todo"
    assert all(key.startswith("{bilibili:user_post}:") for key in q.keys)
    assert q.enqueue(low)
    assert q.enqueue(high)
    assert not q.enqueue(high)
    claim = q.claim("worker-1")
    assert claim.seed.seed_id == high.seed_id
    assert claim.seed.execution_count == claim.execution_count == 1
    assert q.ack(claim)
    assert not q.ack(claim)
    assert not q.enqueue(high)
    assert q.claim("worker-1").seed.seed_id == low.seed_id
    assert q.counts() == {"todo": 0, "processing": 1, "retry": 0, "failed": 0}
    assert q.executions(high.seed_id)[0]["status"] == "succeeded"


def test_competing_workers_claim_once_per_healthy_lease(client):
    q = queue(client)
    for i in range(500):
        q.enqueue(Seed({"number": i}))

    def consume(worker_id):
        own = queue(client)
        seen = []
        while True:
            claim = own.claim(worker_id)
            if claim is None:
                return seen
            seen.append(claim.seed.seed_id)
            assert own.ack(claim)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(consume, [f"worker-{i}" for i in range(8)]))
    seen = [id for batch in results for id in batch]
    assert len(seen) == len(set(seen)) == 500
    assert q.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 0}


def test_deadline_blocks_stale_owner_before_and_after_recovery(client):
    q = queue(client, lease_timeout=0.04)
    seed = Seed({"id": "recover"})
    q.enqueue(seed)
    stale = q.claim("old")
    assert q.is_owned(stale)
    time.sleep(0.07)
    assert not q.is_owned(stale)
    assert not q.ack(stale)
    assert not q.renew(stale)
    assert not q.fail(stale, {"error_type": "stale"})
    assert not q.schedule_retry(stale, {"error_type": "stale"}, delay=0)
    assert not q.record_request(stale, "r", 1, "success")
    assert q.recover() == {"reclaimed": 1, "failed": 0, "promoted": 0}
    current = q.claim("new")
    assert current.seed.seed_id == seed.seed_id
    assert current.token != stale.token
    assert current.execution_count == 2
    assert not q.ack(stale)
    assert q.renew(current)
    assert q.ack(current)


def test_restart_preserves_execution_budget(client):
    q = queue(client, lease_timeout=0.03, max_executions=3)
    seed = Seed({"id": "budget"})
    q.enqueue(seed)
    for execution in range(1, 4):
        # A new queue instance emulates a fresh worker's process state.
        q = queue(client, lease_timeout=0.03, max_executions=3)
        claim = q.claim(f"worker-{execution}")
        assert claim.execution_count == execution
        time.sleep(0.055)
        result = q.recover()
        assert result["reclaimed" if execution < 3 else "failed"] == 1
    assert q.claim("last") is None
    failure = q.failed(seed_id=seed.seed_id)[0]
    assert failure["execution_count"] == 3
    assert len(failure["executions"]) == 3
    assert failure["error"]["execution_budget_exhausted"] is True
    assert q.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 1}


def test_delayed_infrastructure_retry_and_exhaustion(client):
    q = queue(client, max_executions=2)
    seed = Seed({"page": 1})
    q.enqueue(seed)
    claim = q.claim("first")
    error = {"stage": "storage", "error_type": "ConnectionError", "partial_summary": {"items": 2}}
    assert q.schedule_retry(claim, error, delay=0.04)
    assert q.claim("too-soon") is None
    assert q.counts()["retry"] == 1
    assert q.recover()["promoted"] == 0
    time.sleep(0.06)
    assert q.recover()["promoted"] == 1
    claim = q.claim("second")
    assert claim.execution_count == 2
    assert q.schedule_retry(claim, error, delay=0)
    failure = q.failed(seed_id=seed.seed_id)[0]
    assert failure["partial_summary"] == {"items": 2}
    assert failure["error_type"] == "ConnectionError"
    assert failure["error"]["execution_budget_exhausted"] is True


def test_request_trace_partial_failure_filter_and_replay(client):
    q = queue(client)
    seed = Seed({"user_id": "123"}, metadata={"cycle": "20261011"})
    q.enqueue(seed)
    claim = q.claim("worker")
    assert q.record_request(claim, "posts", 1, "attempt")
    assert q.record_request(claim, "posts", 1, "failure", {"error_type": "Timeout"})
    assert q.record_request(claim, "posts", 1, "failure", {"error_type": "Timeout"})
    assert q.record_request(claim, "posts", 2, "success")
    error = {"stage": "parse", "error_type": "MissingField", "message": "posts missing",
             "partial_summary": {"items_inserted": 2, "children_enqueued": 1}}
    assert q.fail(claim, error)
    failed = q.failed(error_type="MissingField")[0]
    assert failed["seed"]["data"] == seed.to_dict()
    assert failed["partial_summary"]["items_inserted"] == 2
    execution = failed["executions"][0]
    assert execution["request_attempts"] == 2
    assert len(execution["requests"]["posts"]["attempts"]) == 3
    assert q.failed(error_type="Other") == []
    replay = q.replay(seed.seed_id)
    assert replay.seed_id != seed.seed_id
    assert replay.execution_count == 0
    assert replay.metadata["replay_of"] == seed.seed_id
    assert replay.metadata["replay_root"] == seed.seed_id
    assert replay.metadata["cycle"] == "20261011"
    assert q.failed(seed_id=seed.seed_id)[0]["replays"][0]["seed_id"] == replay.seed_id
    assert q.counts()["failed"] == 1
    assert q.claim("replay").execution_count == 1


@pytest.mark.parametrize("payload", ["{'id': __import__('os').system('echo unsafe')}", "{}", None])
def test_corrupt_payload_is_retained_as_failure(client, payload):
    q = queue(client)
    seed = Seed({"valid": True})
    q.enqueue(seed)
    if payload is None:
        client.hdel(q.keys[4], seed.seed_id)
    else:
        client.hset(q.keys[4], seed.seed_id, payload)
    assert q.claim("worker") is None
    failure = q.failed()[0]
    assert failure["seed_id"] == seed.seed_id
    assert failure["error_type"] == "InvalidSeed"
    assert q.counts()["failed"] == 1


def test_model_invalid_envelope_becomes_failure(client):
    q = queue(client)
    seed = Seed({"valid": True})
    q.enqueue(seed)
    body = json.loads(seed.to_json())
    body["metadata"] = 123
    client.hset(q.keys[4], seed.seed_id, json.dumps(body))
    assert q.claim("worker") is None
    assert q.failed()[0]["error_type"] == "InvalidSeed"


def test_large_integer_business_values_survive_claim_failure_and_replay(client):
    q = queue(client)
    seed = Seed({"id": 9223372036854775807, "nested": {"count": 12345678901234567890},
                 "empty_array": [], "empty_object": {}})
    q.enqueue(seed)
    claim = q.claim("worker")
    assert claim.seed.to_dict() == seed.to_dict()
    assert q.fail(claim, {"error_type": "Example"})
    assert q.failed()[0]["seed"]["data"] == seed.to_dict()
    replay = q.replay(seed.seed_id)
    assert replay.to_dict() == seed.to_dict()


def test_completed_payloads_removed_and_producers_prune_identity_history(client):
    q = queue(client, dedup_ttl=0.03)
    for i in range(20):
        q.enqueue(Seed({"number": i}))
    for _ in range(20):
        assert q.ack(q.claim("worker"))
    assert client.hlen(q.keys[4]) == 0
    assert client.hlen(q.keys[5]) == 0
    assert client.hlen(q.keys[11]) == 0
    assert client.hlen(q.keys[7]) == 20
    time.sleep(0.05)
    q.enqueue(Seed({"new": True}))
    assert client.hlen(q.keys[7]) == 0
    assert client.zcard(q.keys[8]) == 1


def test_completed_identity_window_expires_but_active_seed_stays_protected(client):
    q = queue(client, dedup_ttl=0.04)
    done, active = Seed({"done": True}), Seed({"active": True})
    q.enqueue(done)
    claim = q.claim("worker")
    assert q.ack(claim)
    q.enqueue(active)
    time.sleep(0.065)
    assert not q.enqueue(active)
    q.recover()
    assert q.executions(done.seed_id) == []
    assert q.enqueue(done)


def test_worker_heartbeat_expires_by_server_clock(client):
    q = queue(client)
    q.heartbeat("worker", {"inflight": 2}, ttl=0.03)
    workers = q.workers()
    assert workers[0]["worker_id"] == "worker"
    assert workers[0]["inflight"] == 2
    assert workers[0]["project"] == "bilibili"
    time.sleep(0.055)
    assert q.workers() == []


def test_replay_of_a_failed_replay_preserves_original_lineage(client):
    q = queue(client)
    seed = Seed({"page": 1})
    q.enqueue(seed)
    q.fail(q.claim("first"), {"error_type": "Example"})
    first_replay = q.replay(seed.seed_id)
    q.fail(q.claim("second"), {"error_type": "Example"})
    next_replay = q.replay(first_replay.seed_id)
    assert next_replay.metadata["replay_of"] == first_replay.seed_id
    assert next_replay.metadata["replay_root"] == seed.seed_id


def test_manual_replay_reuses_stable_child_identity(client):
    q = queue(client)
    child_queue = RedisQueue(client, "bilibili", "video_detail")
    parent = Seed({"page": 1})
    q.enqueue(parent)
    original = q.claim("first")
    child = Seed({"bvid": "BV123"}).derive_id(original.seed, "bilibili", "video_detail")
    assert child_queue.enqueue(child)
    assert q.fail(original, {"error_type": "PartialFailure", "partial_summary": {"children_enqueued": 1}})
    replay = q.replay(parent.seed_id)
    replay_child = Seed({"bvid": "BV123"}).derive_id(replay, "bilibili", "video_detail")
    assert child.seed_id == replay_child.seed_id
    assert not child_queue.enqueue(replay_child)


def test_wrapper_has_normal_redis_semantics(redis_socket):
    wrapper = RedisDB.from_url("unix://" + redis_socket)
    try:
        assert wrapper.ping()
        wrapper.set("wrapper", "value")
        assert wrapper.get("wrapper") == "value"
        assert repr(wrapper) == "RedisDB()"
        with pytest.raises(redis.DataError):
            wrapper.set("unencoded-dict", {"invalid": True})
    finally:
        wrapper.close()
    with pytest.raises(ValueError):
        RedisDB("")
    with pytest.raises(ValueError):
        RedisDB("redis://localhost", decode_responses=False)

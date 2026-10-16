"""Atomic, leased priority queues using the framework's versioned Seed JSON.

Redis server time governs ownership. All keys share a task hash tag. This is
at-least-once processing: committing output and acknowledging a lease are
separate operations. Execution history is bounded per Seed and expires after
the identity retention window, except that final failure records are retained.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
from typing import Any
from uuid import uuid4

from spider_man.network.seed import Seed


@dataclass
class Claim:
    seed: Seed
    token: str
    worker_id: str
    execution_count: int
    deadline: float


# KEYS: todo, processing, retry, failed, payloads, claims, failures,
# executions, seen, workers, worker_info, execution_counts. No arbitrary executable objects are
# stored in Redis. JSON parsing uses cjson in Redis and json in Python.
_COMMON = r"""
local clock = redis.call('TIME')
local now = tonumber(clock[1]) + tonumber(clock[2]) / 1000000
local ttl = tonumber(ARGV[1])
local budget = tonumber(ARGV[2])
local function decode(raw)
    if not raw then return nil end
    local ok, value = pcall(cjson.decode, raw)
    if ok and type(value) == 'table' then return value end
    return nil
end
local function history(id)
    return decode(redis.call('HGET', KEYS[8], id)) or {}
end
local function save_history(id, records)
    -- Retain only the configured execution budget (plus one defensive entry).
    while #records > budget + 1 do table.remove(records, 1) end
    redis.call('HSET', KEYS[8], id, cjson.encode(records))
end
local function ownership(id, token, worker, count)
    local claim = decode(redis.call('HGET', KEYS[6], id))
    if not claim or claim.token ~= token or claim.worker_id ~= worker
       or claim.execution_count ~= tonumber(count)
       or tonumber(claim.deadline or 0) <= now then return nil end
    return claim
end
local function finish(id, status, error)
    local records = history(id)
    if #records > 0 then
        local record = records[#records]
        record.status = status
        record.finished_at = now
        if error then record.error = error end
        save_history(id, records)
    end
    return records
end
local function forget_claim(id)
    redis.call('ZREM', KEYS[2], id)
    redis.call('HDEL', KEYS[6], id)
end
local function failure(id, raw, error, count)
    local payload = decode(raw)
    local old = decode(redis.call('HGET', KEYS[7], id))
    local records = finish(id, 'failed', error)
    local failed = {
        seed_id=id, seed_json=raw or cjson.null,
        project=ARGV[3], task=ARGV[4], execution_count=count or 0,
        error=error, error_type=error.error_type or error.type or 'SeedError',
        stage=error.stage or 'seed', first_failed_at=(old and old.first_failed_at) or now,
        last_failed_at=now, executions=records,
        partial_summary=error.partial_summary or error.partial or cjson.null
    }
    if not payload then failed.raw_payload = raw or cjson.null end
    redis.call('HSET', KEYS[7], id, cjson.encode(failed))
    redis.call('ZADD', KEYS[4], now, id)
    redis.call('ZREM', KEYS[1], id)
    redis.call('ZREM', KEYS[3], id)
    forget_claim(id)
    -- The failure record owns the original payload and execution history.
    redis.call('HDEL', KEYS[5], id)
    redis.call('HDEL', KEYS[12], id)
    redis.call('ZADD', KEYS[9], now + ttl, id)
end
local function prune_history(limit)
    local obsolete = redis.call('ZRANGEBYSCORE', KEYS[9], '-inf', now, 'LIMIT', 0, limit)
    for _, id in ipairs(obsolete) do
        if redis.call('HEXISTS', KEYS[5], id) == 1 then
            redis.call('ZADD', KEYS[9], now + ttl, id)
        else
            redis.call('ZREM', KEYS[9], id)
            redis.call('HDEL', KEYS[8], id)
            redis.call('HDEL', KEYS[12], id)
        end
    end
end
"""

_ENQUEUE = _COMMON + r"""
-- Producers also sweep completed identities so sustained throughput cannot
-- outpace a maintenance scan's fixed batch size after the retention window.
prune_history(100)
local id, raw, priority = ARGV[5], ARGV[6], tonumber(ARGV[7])
local seen = tonumber(redis.call('ZSCORE', KEYS[9], id) or 0)
if seen > now or redis.call('HEXISTS', KEYS[5], id) == 1
   or redis.call('HEXISTS', KEYS[7], id) == 1 then return 0 end
redis.call('HDEL', KEYS[8], id)
redis.call('HSET', KEYS[5], id, raw)
redis.call('HSET', KEYS[12], id, ARGV[8])
redis.call('ZADD', KEYS[1], priority, id)
redis.call('ZADD', KEYS[9], now + ttl, id)
return 1
"""

_CLAIM = _COMMON + r"""
prune_history(100)
for i = 1, 100 do
    local ids = redis.call('ZRANGE', KEYS[1], 0, 0)
    if #ids == 0 then return false end
    local id = ids[1]
    local raw = redis.call('HGET', KEYS[5], id)
    local payload = decode(raw)
    if not payload or payload.version ~= 1 or payload.seed_id ~= id
       or type(payload.data) ~= 'table' or type(payload.priority) ~= 'number'
       or type(payload.execution_count) ~= 'number'
       or payload.execution_count < 0 or payload.execution_count % 1 ~= 0 then
        failure(id, raw, {stage='queue', error_type='InvalidSeed',
            message='Missing or corrupt versioned Seed JSON'}, 0)
    elseif tonumber(redis.call('HGET', KEYS[12], id) or payload.execution_count) >= budget then
        failure(id, raw, {stage='recovery', error_type='ExecutionBudgetExceeded',
            message='Seed execution budget exhausted'}, tonumber(redis.call('HGET', KEYS[12], id) or payload.execution_count))
    else
        local count = tonumber(redis.call('HGET', KEYS[12], id) or payload.execution_count) + 1
        local deadline = now + tonumber(ARGV[7])
        local claim = {seed_id=id, token=ARGV[6], worker_id=ARGV[5],
            execution_count=count, deadline=deadline}
        local records = history(id)
        table.insert(records, {execution_count=count,
            worker_id=ARGV[5], started_at=now, status='processing',
            requests={}, request_attempts=0, request_trace_dropped=0})
        save_history(id, records)
        redis.call('HSET', KEYS[12], id, count)
        redis.call('HSET', KEYS[6], id, cjson.encode(claim))
        redis.call('ZREM', KEYS[1], id)
        redis.call('ZADD', KEYS[2], deadline, id)
        redis.call('ZADD', KEYS[9], now + ttl, id)
        return cjson.encode({payload=raw, claim=claim})
    end
end
return false
"""

_RENEW = _COMMON + r"""
local claim = ownership(ARGV[5], ARGV[6], ARGV[7], ARGV[8])
if not claim then return false end
claim.deadline = now + tonumber(ARGV[9])
redis.call('HSET', KEYS[6], ARGV[5], cjson.encode(claim))
redis.call('ZADD', KEYS[2], claim.deadline, ARGV[5])
redis.call('ZADD', KEYS[9], now + ttl, ARGV[5])
return tostring(claim.deadline)
"""

_ACK = _COMMON + r"""
local id = ARGV[5]
if not ownership(id, ARGV[6], ARGV[7], ARGV[8]) then return 0 end
finish(id, 'succeeded', nil)
forget_claim(id)
redis.call('HDEL', KEYS[5], id)
redis.call('HDEL', KEYS[12], id)
redis.call('ZADD', KEYS[9], now + ttl, id)
return 1
"""

_OWNED = _COMMON + r"""
return ownership(ARGV[5], ARGV[6], ARGV[7], ARGV[8]) and 1 or 0
"""

_FAIL = _COMMON + r"""
local id = ARGV[5]
local claim = ownership(id, ARGV[6], ARGV[7], ARGV[8])
if not claim then return 0 end
failure(id, redis.call('HGET', KEYS[5], id), decode(ARGV[9]), claim.execution_count)
return 1
"""

_RETRY = _COMMON + r"""
local id = ARGV[5]
local claim = ownership(id, ARGV[6], ARGV[7], ARGV[8])
if not claim then return 0 end
local raw = redis.call('HGET', KEYS[5], id)
local error = decode(ARGV[9])
if claim.execution_count >= budget then
    error.execution_budget_exhausted = true
    failure(id, raw, error, claim.execution_count)
else
    finish(id, 'retry', error)
    forget_claim(id)
    redis.call('ZADD', KEYS[3], now + tonumber(ARGV[10]), id)
    redis.call('ZADD', KEYS[9], now + ttl, id)
end
return 1
"""

_RECOVER = _COMMON + r"""
local reclaimed, failed, promoted = 0, 0, 0
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 1000)
for _, id in ipairs(expired) do
    local claim = decode(redis.call('HGET', KEYS[6], id))
    local raw = redis.call('HGET', KEYS[5], id)
    local payload = decode(raw)
    -- A stale ZSet score cannot invalidate an otherwise healthy claim.
    if claim and tonumber(claim.deadline or 0) > now then
        redis.call('ZADD', KEYS[2], claim.deadline, id)
    else
        local error = {stage='recovery', error_type='WorkerLeaseExpired',
            message='Worker lease expired before completion'}
        if not payload or type(payload.execution_count) ~= 'number'
           or type(payload.priority) ~= 'number' then
            error.error_type = 'InvalidSeed'
            error.message = 'Missing or corrupt Seed payload during recovery'
            failure(id, raw, error, 0)
            failed = failed + 1
        elseif tonumber(redis.call('HGET', KEYS[12], id) or payload.execution_count) >= budget then
            error.execution_budget_exhausted = true
            failure(id, raw, error, tonumber(redis.call('HGET', KEYS[12], id) or payload.execution_count))
            failed = failed + 1
        else
            finish(id, 'recovered', error)
            forget_claim(id)
            redis.call('ZADD', KEYS[1], payload.priority, id)
            redis.call('ZADD', KEYS[9], now + ttl, id)
            reclaimed = reclaimed + 1
        end
    end
end
local due = redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 1000)
for _, id in ipairs(due) do
    local raw = redis.call('HGET', KEYS[5], id)
    local payload = decode(raw)
    if not payload or type(payload.priority) ~= 'number' then
        failure(id, raw, {stage='queue', error_type='InvalidSeed',
            message='Missing or corrupt Seed payload during retry promotion'}, 0)
        failed = failed + 1
    else
        redis.call('ZREM', KEYS[3], id)
        redis.call('ZADD', KEYS[1], payload.priority, id)
        redis.call('ZADD', KEYS[9], now + ttl, id)
        promoted = promoted + 1
    end
end
-- Bound completed history and identity retention; active tasks stay protected.
prune_history(1000)
return cjson.encode({reclaimed=reclaimed, failed=failed, promoted=promoted})
"""

_REQUEST = _COMMON + r"""
local id = ARGV[5]
if not ownership(id, ARGV[6], ARGV[7], ARGV[8]) then return 0 end
local records = history(id)
if #records == 0 then return 0 end
local record = records[#records]
local requests = record.requests or {}
local request_id = ARGV[9]
local request = requests[request_id]
if not request then
    local count = 0
    for _ in pairs(requests) do count = count + 1 end
    if count >= 1000 then
        record.request_trace_dropped = (record.request_trace_dropped or 0) + 1
        if ARGV[11] == 'attempt' then
            record.request_attempts = (record.request_attempts or 0) + 1
        end
        save_history(id, records)
        return 1
    end
    request = {request_id=request_id, attempts={}}
    requests[request_id] = request
end
local attempt = tonumber(ARGV[10])
local event = {attempt=attempt, status=ARGV[11], recorded_at=now}
if ARGV[12] ~= '' then event.error = decode(ARGV[12]) end
-- A lost response may cause the same event to be recorded again. Replace its
-- attempt/status entry rather than count a phantom network attempt.
local duplicate = false
for i, previous in ipairs(request.attempts) do
    if previous.attempt == attempt and previous.status == event.status then
        request.attempts[i] = event
        duplicate = true
        break
    end
end
if not duplicate then table.insert(request.attempts, event) end
if attempt > (request.attempt_count or 0) then
    record.request_attempts = (record.request_attempts or 0) + attempt - (request.attempt_count or 0)
    request.attempt_count = attempt
end
while #request.attempts > 16 do table.remove(request.attempts, 1) end
request.last_status = event.status
request.last_error = event.error or cjson.null
record.requests = requests
save_history(id, records)
return 1
"""

_REPLAY = _COMMON + r"""
local old = decode(redis.call('HGET', KEYS[7], ARGV[5]))
if not old then return 0 end
local id, raw, priority = ARGV[6], ARGV[7], tonumber(ARGV[8])
if redis.call('HEXISTS', KEYS[5], id) == 1 or redis.call('ZSCORE', KEYS[9], id) then return 0 end
redis.call('HSET', KEYS[5], id, raw)
redis.call('HSET', KEYS[12], id, 0)
redis.call('ZADD', KEYS[1], priority, id)
redis.call('ZADD', KEYS[9], now + ttl, id)
local replays = old.replays or {}
table.insert(replays, {seed_id=id, created_at=now})
while #replays > 100 do table.remove(replays, 1) end
old.replays = replays
old.replay_count = (old.replay_count or 0) + 1
redis.call('HSET', KEYS[7], ARGV[5], cjson.encode(old))
return 1
"""

_HEARTBEAT = _COMMON + r"""
local info = decode(ARGV[6])
info.worker_id = ARGV[5]
info.project = ARGV[3]
info.task = ARGV[4]
info.heartbeat_at = now
info.expires_at = now + tonumber(ARGV[7])
redis.call('HSET', KEYS[11], ARGV[5], cjson.encode(info))
redis.call('ZADD', KEYS[10], info.expires_at, ARGV[5])
return 1
"""

_WORKERS = _COMMON + r"""
local expired = redis.call('ZRANGEBYSCORE', KEYS[10], '-inf', now)
for _, id in ipairs(expired) do
    redis.call('ZREM', KEYS[10], id)
    redis.call('HDEL', KEYS[11], id)
end
local result = {}
for _, id in ipairs(redis.call('ZRANGE', KEYS[10], 0, -1)) do
    local info = decode(redis.call('HGET', KEYS[11], id))
    if info then table.insert(result, info) end
end
return cjson.encode(result)
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


class RedisQueue:
    def __init__(self, client: Any, project: str, task: str, *,
                 lease_timeout: float = 120, max_executions: int = 3,
                 dedup_ttl: float = 604800) -> None:
        if not all(isinstance(v, str) and v and not any(c in v for c in "{}:")
                   for v in (project, task)):
            raise ValueError("project and task must be non-empty and contain no braces or colons")
        if not math.isfinite(lease_timeout) or lease_timeout <= 0:
            raise ValueError("lease_timeout must be positive")
        if not isinstance(max_executions, int) or isinstance(max_executions, bool) or max_executions < 1:
            raise ValueError("max_executions must be a positive integer")
        if not math.isfinite(dedup_ttl) or dedup_ttl <= 0:
            raise ValueError("dedup_ttl must be positive")
        self.client = client.client if hasattr(client, "client") and not callable(client.client) else client
        self.project, self.task = project, task
        self.lease_timeout, self.max_executions, self.dedup_ttl = lease_timeout, max_executions, dedup_ttl
        self.prefix = "{" + project + ":" + task + "}"
        self.keys = [self.prefix + ":" + suffix for suffix in (
            "todo", "processing", "retry", "failed", "payloads", "claims",
            "failures", "executions", "seen", "workers", "worker_info", "execution_counts")]
        self.todo_key, self.processing_key, self.retry_key, self.failed_key = self.keys[:4]
        self._scripts = {name: self.client.register_script(code) for name, code in {
            "enqueue": _ENQUEUE, "claim": _CLAIM, "renew": _RENEW, "ack": _ACK,
            "owned": _OWNED,
            "fail": _FAIL, "retry": _RETRY, "recover": _RECOVER, "request": _REQUEST,
            "replay": _REPLAY, "heartbeat": _HEARTBEAT, "workers": _WORKERS}.items()}

    def _call(self, name: str, *args: Any) -> Any:
        return self._scripts[name](keys=self.keys, args=[
            self.dedup_ttl, self.max_executions, self.project, self.task, *args])

    @staticmethod
    def _owner(claim: Claim) -> tuple[Any, ...]:
        return claim.seed.seed_id, claim.token, claim.worker_id, claim.execution_count

    def enqueue(self, seed: Seed) -> bool:
        if seed.project not in (None, self.project) or seed.task not in (None, self.task):
            raise ValueError("Seed destination does not match this queue")
        return bool(self._call("enqueue", seed.seed_id, seed.to_json(), seed.priority, seed.execution_count))

    def claim(self, worker_id: str) -> Claim | None:
        if not isinstance(worker_id, str) or not worker_id:
            raise ValueError("worker_id is required")
        raw = self._call("claim", worker_id, uuid4().hex, self.lease_timeout)
        if not raw:
            return None
        result = json.loads(raw)
        record = result["claim"]
        try:
            seed = Seed.from_json(result["payload"])
        except (ValueError, TypeError, KeyError) as exc:
            # The atomic claim preserves this payload. A model-level validation
            # error becomes a retained failure and never drops the seed silently.
            placeholder = Seed({}, seed_id=record["seed_id"])
            invalid = Claim(placeholder, record["token"], worker_id,
                            record["execution_count"], record["deadline"])
            self.fail(invalid, {"stage": "queue", "error_type": "InvalidSeed", "message": str(exc)})
            return None
        seed.execution_count = record["execution_count"]
        return Claim(seed, record["token"], worker_id, record["execution_count"], record["deadline"])

    def renew(self, claim: Claim) -> bool:
        deadline = self._call("renew", *self._owner(claim), self.lease_timeout)
        if not deadline:
            return False
        claim.deadline = float(deadline)
        return True

    def ack(self, claim: Claim) -> bool:
        return bool(self._call("ack", *self._owner(claim)))

    def is_owned(self, claim: Claim) -> bool:
        """Check server-time ownership before starting an external output.

        This fences a previously expired owner, but cannot provide a transaction
        with a write already in flight against another database.
        """
        return bool(self._call("owned", *self._owner(claim)))

    def fail(self, claim: Claim, error: dict[str, Any]) -> bool:
        if not isinstance(error, dict):
            raise TypeError("error must be a JSON dictionary")
        return bool(self._call("fail", *self._owner(claim), _json(error)))

    def schedule_retry(self, claim: Claim, error: dict[str, Any], delay: float = 1) -> bool:
        if not isinstance(error, dict):
            raise TypeError("error must be a JSON dictionary")
        if not math.isfinite(delay) or delay < 0:
            raise ValueError("retry delay must be non-negative")
        return bool(self._call("retry", *self._owner(claim), _json(error), delay))

    def recover(self) -> dict[str, int]:
        return json.loads(self._call("recover"))

    def counts(self) -> dict[str, int]:
        pipe = self.client.pipeline(transaction=True)
        for key in self.keys[:4]:
            pipe.zcard(key)
        return dict(zip(("todo", "processing", "retry", "failed"), pipe.execute()))

    def failed(self, limit: int = 100, error_type: str | None = None,
               seed_id: str | None = None) -> list[dict[str, Any]]:
        if not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise ValueError("limit must be between 1 and 10000")
        if seed_id is not None:
            raw = self.client.hget(self.keys[6], seed_id)
            result = [self._failure_record(raw)] if raw else []
            return [f for f in result if error_type is None or f["error_type"] == error_type]
        result: list[dict[str, Any]] = []
        offset = 0
        while len(result) < limit:
            ids = self.client.zrevrange(self.failed_key, offset, offset + 99)
            if not ids:
                break
            offset += len(ids)
            for raw in self.client.hmget(self.keys[6], ids):
                if raw:
                    failure = self._failure_record(raw)
                    if error_type is None or failure["error_type"] == error_type:
                        result.append(failure)
                        if len(result) == limit:
                            break
        return result

    @staticmethod
    def _failure_record(raw: str) -> dict[str, Any]:
        failure = json.loads(raw)
        original = failure.get("seed_json")
        try:
            seed = json.loads(original) if original else None
        except (ValueError, TypeError):
            seed = None
        if isinstance(seed, dict):
            seed["execution_count"] = failure["execution_count"]
        failure["seed"] = seed
        return failure

    def replay(self, seed_id: str) -> Seed | None:
        found = self.failed(seed_id=seed_id, limit=1)
        if not found:
            return None
        old = found[0]["seed"]
        if not isinstance(old, dict):
            raise ValueError("Corrupt Seed cannot be replayed; repair and enqueue valid JSON")
        original = Seed.from_json(_json(old))
        metadata = dict(original.metadata)
        metadata.update(replay_of=seed_id,
                        replay_root=original.metadata.get("replay_root", original.metadata.get("replay_of", seed_id)),
                        replay_dedup_window_seconds=self.dedup_ttl,
                        replay_note="Child identity records beyond the retention window may have expired")
        replay = Seed(original.to_dict(), project=self.project, task=self.task,
                      priority=original.priority, metadata=metadata)
        if not self._call("replay", seed_id, replay.seed_id, replay.to_json(), replay.priority):
            return None
        return replay

    def record_request(self, claim: Claim, request_id: str, attempt: int,
                       status: str, error: dict[str, Any] | None = None) -> bool:
        if not isinstance(request_id, str) or not request_id or not isinstance(status, str) or not status:
            raise ValueError("request_id and status are required")
        if not isinstance(attempt, int) or isinstance(attempt, bool) or attempt < 1:
            raise ValueError("attempt must be a positive integer")
        if error is not None and not isinstance(error, dict):
            raise TypeError("request error must be a JSON dictionary")
        return bool(self._call("request", *self._owner(claim), request_id,
                              attempt, status, _json(error) if error is not None else ""))

    def executions(self, seed_id: str) -> list[dict[str, Any]]:
        raw = self.client.hget(self.keys[7], seed_id)
        records = json.loads(raw) if raw else []
        return records if isinstance(records, list) else []

    def heartbeat(self, worker_id: str, info: dict[str, Any], ttl: float = 60) -> None:
        if not isinstance(worker_id, str) or not worker_id:
            raise ValueError("worker_id is required")
        if not isinstance(info, dict):
            raise TypeError("heartbeat info must be a JSON dictionary")
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("heartbeat ttl must be positive")
        self._call("heartbeat", worker_id, _json(info), ttl)

    def workers(self) -> list[dict[str, Any]]:
        result = json.loads(self._call("workers"))
        return result if isinstance(result, list) else []

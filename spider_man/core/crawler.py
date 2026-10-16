from __future__ import annotations

import hashlib
import logging
import os
import queue as thread_queue
import random
import signal
import socket
import threading
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass, field

from redis.exceptions import RedisError

from spider_man.network.item import Item
from spider_man.network.request import Request
from spider_man.network.seed import Seed
from spider_man.utils.serialization import validate_name
from spider_man.config import Settings, import_object
from spider_man.exceptions import ConfigurationError, DownloadError, LeaseLost, RetryableError
from spider_man.utils.log import TaskLogger, safe_error
from spider_man.utils.metrics import Metrics


@dataclass
class Execution:
    claim: object
    lost: threading.Event = field(default_factory=threading.Event)
    partial: dict = field(default_factory=lambda: {"items_confirmed": 0, "children_enqueued": 0, "requests_completed": 0})
    stage: str = "request"
    request_summary: dict | None = None
    pending_items: deque = field(default_factory=deque)
    request_attempts: dict = field(default_factory=dict)
    request_limits: dict = field(default_factory=dict)


class Crawler:
    def __init__(self, project, task, *, custom_settings=None,
                 redis=None, mongo=None, cli_settings=None):
        self.project, self.task = validate_name(project, "project"), validate_name(task, "task")
        self.settings = Settings(custom_settings=custom_settings, cli=cli_settings)
        source = self.settings["LOG_SOURCE"]
        topic = self.settings["LOG_TOPIC"]
        self.collection_name = f"{self.project if source is None else source}_{self.task if topic is None else topic}"
        self.todo_key = f"{{{project}:{task}}}:todo"
        self.worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        self.metrics = Metrics()
        self.log = TaskLogger(project, task, self.worker_id, self.settings)
        self.redis, self.mongo = redis, mongo
        self._owns_redis, self._owns_mongo = redis is None, mongo is None
        self.queue = None
        self.pipeline = None
        self.downloader = None
        self._request_callback = None
        self._parse_callbacks = {}
        self._start_hooks = []
        self._stop = threading.Event()
        self._maintenance_stop = threading.Event()
        self._contexts = {}
        self._context_lock = threading.Lock()
        self._local = threading.local()
        self._http_slots = threading.BoundedSemaphore(self.settings["REQUEST_CONCURRENCY"])
        self._rate_lock = threading.Lock()
        self._next_request = 0.0
        self._running = False
        self._retry_exceptions = ()

    def request(self, function):
        if self._running or self._request_callback is not None:
            raise ConfigurationError("Register one request callback before starting")
        self._request_callback = function
        return function

    def parse(self, function=None, *, name=None):
        if isinstance(function, str):
            return lambda callback: self.parse(callback, name=function)
        if function is None:
            return lambda callback: self.parse(callback, name=name)
        key = name or "default"
        if self._running or key in self._parse_callbacks:
            raise ConfigurationError(f"Duplicate or late parse callback: {key}")
        self._parse_callbacks[key] = function
        return function

    def on_start(self, function):
        if self._running:
            raise ConfigurationError("Register startup hooks before starting")
        self._start_hooks.append(function)
        return function

    def _connect_redis(self):
        if self.redis is None:
            from spider_man.db.redisdb import RedisDB
            self.settings.validate_connections()
            self.redis = RedisDB(url=self.settings["REDIS_URL"], socket_connect_timeout=5, socket_timeout=5)
        if self.queue is None:
            from spider_man.core.queue import RedisQueue
            self.queue = RedisQueue(self.redis, self.project, self.task,
                                    lease_timeout=self.settings["LEASE_TIMEOUT"],
                                    max_executions=self.settings["SEED_MAX_EXECUTIONS"],
                                    dedup_ttl=self.settings["DEDUP_TTL"])

    def _connect(self):
        self.settings.validate_connections()
        self._connect_redis()
        if self.mongo is None:
            from spider_man.db.mongodb import MongoDB
            self.mongo = MongoDB(url=self.settings["MONGO_URI"], db=self.settings["MONGO_DATABASE"],
                                 serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, socketTimeoutMS=5000, retryWrites=False)
        self.redis.ping()
        self.mongo.ping()

    def _target_queue(self, project, task):
        if (project, task) == (self.project, self.task):
            return self.queue
        from spider_man.core.queue import RedisQueue
        return RedisQueue(self.redis, project, task,
                          lease_timeout=self.settings["LEASE_TIMEOUT"],
                          max_executions=self.settings["SEED_MAX_EXECUTIONS"],
                          dedup_ttl=self.settings["DEDUP_TTL"])

    def enqueue(self, seed, *, project=None, task=None, parent=None):
        context = getattr(self._local, "context", None)
        if context is not None:
            self._assert_owned(context, remote=True)
            self._await_items(context)
            self._assert_owned(context, remote=True)
            parent = parent or context.claim.seed
        if not isinstance(seed, Seed):
            seed = Seed(seed)
        self._connect_redis()
        project = validate_name(project or seed.project or self.project, "project")
        task = validate_name(task or seed.task or self.task, "task")
        if parent is not None:
            seed.derive_id(parent, project, task)
        seed.project, seed.task = project, task
        inserted = self._target_queue(project, task).enqueue(seed)
        self.metrics.counter("seed_enqueued" if inserted else "seed_duplicate")
        if context is not None:
            context.partial["children_enqueued"] += 1
        return inserted

    def start_seeds(self, seeds):
        counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
        for value in seeds:
            try:
                counts["accepted" if self.enqueue(value) else "duplicate"] += 1
            except (ValueError, TypeError) as exc:
                counts["rejected"] += 1
                self.log.event("seed_rejected", level=logging.WARNING, error=safe_error(exc))
        return counts

    def check(self):
        """Read-only: never run hooks or open pipelines that may write."""
        self._connect()
        extensions = {}
        paths = [self.settings["DOWNLOADER"], *self.settings["ITEM_PIPELINES"]]
        if self.settings["PROXY_PROVIDER"]:
            paths.append(self.settings["PROXY_PROVIDER"])
        paths.extend(self.settings["RETRY_EXCEPTIONS"])
        for path in paths:
            extension = import_object(path)
            if not callable(extension):
                raise ConfigurationError(f"Extension {path} must be callable")
            if path in self.settings["RETRY_EXCEPTIONS"] and (not isinstance(extension, type) or not issubclass(extension, Exception)):
                raise ConfigurationError("RETRY_EXCEPTIONS must refer to Exception subclasses")
            extensions[path] = "importable"
        from spider_man.pipelines.mongo_pipeline import MongoPipeline
        strategy = MongoPipeline(self)
        try:
            strategy.open()
            result = {"ready": True, "collection": self.collection_name,
                      "upsert_keys": getattr(strategy, "upsert_keys", None)}
        except ConfigurationError as exc:
            result = {"ready": False, "collection": self.collection_name, "error": safe_error(exc)}
        result["startup_hooks_pending"] = len(self._start_hooks)
        result["notice"] = "Startup hooks are not executed; they may create required indexes during run."
        result["config"] = self.settings.summary()
        result["extensions"] = extensions
        result["queue"] = self.queue.counts()
        return result

    def stop(self):
        self._stop.set()

    def _assert_owned(self, context, remote=False):
        if remote and not context.lost.is_set() and not self.queue.is_owned(context.claim):
            context.lost.set()
        if context.lost.is_set():
            raise LeaseLost("Worker no longer owns this execution")

    @staticmethod
    def _outputs(result):
        if result is None:
            return iter(())
        if isinstance(result, (Seed, Request, Item)):
            return iter((result,))
        if isinstance(result, (str, bytes, dict)):
            raise TypeError(f"Unsupported callback output {type(result).__name__}")
        return iter(result)

    def _rate_limit(self, context):
        with self._rate_lock:
            now = time.monotonic()
            wait = max(0, self._next_request - now)
            self._next_request = max(now, self._next_request) + self.settings["REQUEST_INTERVAL"]
        if context.lost.wait(wait):
            self._assert_owned(context)

    def _record(self, context, request, attempt, status, error=None):
        if not self.queue.record_request(context.claim, request.request_id, attempt, status, error):
            context.lost.set()
            raise LeaseLost("Request trace rejected by expired lease")

    def _await_items(self, context, *, all_items=True):
        prior_stage = context.stage
        if context.pending_items:
            context.stage = "storage"
        first_error = None
        processed = 0
        while context.pending_items:
            future = context.pending_items[0]
            if not all_items and processed and not future.done():
                break
            while not future.done():
                context.lost.wait(0.05)
                self._assert_owned(context)
            context.pending_items.popleft()
            processed += 1
            try:
                result = future.result()
                if result.status != "dropped":
                    context.partial["items_confirmed"] += 1
            except Exception as exc:
                first_error = first_error or exc
        if first_error:
            raise first_error
        context.stage = prior_stage

    def _download_parse(self, request, context):
        from spider_man.network.downloader.http_downloader import safe_request_summary
        callback = self._parse_callbacks.get(request.callback or "default")
        if callback is None:
            raise ConfigurationError(f"Unregistered parse callback {request.callback or 'default'}")
        can_retry = request.retryable if request.retryable is not None else request.method in ("GET", "HEAD", "OPTIONS")
        extra = request.max_retries if request.max_retries is not None else self.settings["REQUEST_MAX_RETRIES"]
        if request.request_id not in context.request_limits and len(context.request_limits) >= self.settings["SEED_REQUEST_LIMIT"]:
            raise ConfigurationError("SEED_REQUEST_LIMIT exceeded; split work into child Seeds")
        max_attempts = context.request_limits.setdefault(request.request_id, 1 + (extra if can_retry else 0))
        context.request_summary = safe_request_summary(request)
        first_attempt = context.request_attempts.get(request.request_id, 0) + 1
        if first_attempt > max_attempts:
            self.metrics.counter("request_failure")
            raise DownloadError("Logical request execution budget exhausted", error_type="request_budget", retryable=False)
        for attempt in range(first_attempt, max_attempts + 1):
            self._assert_owned(context)
            context.request_attempts[request.request_id] = attempt
            request.meta["_attempt"] = attempt
            context.stage = "download"
            self._record(context, request, attempt, "attempt")
            while not self._http_slots.acquire(timeout=0.1):
                self._assert_owned(context)
            try:
                self._rate_limit(context)
                self._assert_owned(context, remote=True)
                self.metrics.counter("request_attempt")
                result = self.downloader.download(request)
                self.metrics.observe("request_elapsed", result.elapsed)
                self.metrics.counter(f"request_status_{result.status_code // 100}xx")
            except DownloadError as exc:
                failure = exc
                if exc.status_code is not None:
                    self.metrics.counter(f"request_status_{exc.status_code // 100}xx")
            except Exception as exc:
                if isinstance(exc, self._retry_exceptions):
                    failure = DownloadError(safe_error(exc), error_type=type(exc).__name__, retryable=True)
                else:
                    self.metrics.counter("request_failure")
                    self._record(context, request, attempt, "failure", {"error_type": type(exc).__name__, "message": safe_error(exc)})
                    raise
            else:
                failure = None
            finally:
                self._http_slots.release()
            if failure is None:
                context.stage = "parse"
                try:
                    observed = False
                    for position, output in enumerate(self._outputs(callback(result))):
                        if not isinstance(output, (Seed, Request, Item)):
                            raise TypeError(f"Unsupported callback output {type(output).__name__}")
                        if isinstance(output, Request) and output.request_id is None:
                            output.request_id = hashlib.sha256(f"{request.request_id}:parse:{position}".encode()).hexdigest()
                        observed = True
                        yield output
                        context.stage = "parse"
                except RetryableError as exc:
                    failure = exc
                except Exception as exc:
                    self.metrics.counter("request_failure")
                    self._record(context, request, attempt, "failure", {"error_type": type(exc).__name__, "message": safe_error(exc)})
                    raise
                else:
                    if not observed:
                        self.metrics.counter("empty_result")
                    self.metrics.counter("request_success")
                    self._record(context, request, attempt, "success")
                    context.partial["requests_completed"] += 1
                    return
            retry = bool(getattr(failure, "retryable", False)) and attempt < max_attempts
            self._record(context, request, attempt, "retry" if retry else "failure", {
                "error_type": getattr(failure, "error_type", type(failure).__name__), "message": safe_error(failure),
                "status_code": getattr(failure, "status_code", None)})
            if not retry:
                self.metrics.counter("request_failure")
                raise failure
            self.metrics.counter("request_retry")
            delay = min(self.settings["REQUEST_RETRY_BACKOFF_MAX"],
                        self.settings["REQUEST_RETRY_BACKOFF"] * 2 ** (attempt - 1))
            after = getattr(failure, "retry_after", None)
            delay = max(min(delay * random.uniform(0.8, 1.2), self.settings["REQUEST_RETRY_BACKOFF_MAX"]),
                        min(after or 0, self.settings["RETRY_AFTER_MAX"]))
            if context.lost.wait(delay):
                self._assert_owned(context)

    def _root_outputs(self, seed):
        for position, output in enumerate(self._outputs(self._request_callback(seed))):
            if isinstance(output, Request) and output.request_id is None:
                output.request_id = hashlib.sha256(f"{seed.seed_id}:request:{position}".encode()).hexdigest()
            yield output

    def _execute(self, context):
        seed = context.claim.seed
        try:
            self._local.context = context
            self._assert_owned(context, remote=True)
            context.stage = "request"
            stack = [self._root_outputs(seed)]
            while stack:
                self._assert_owned(context)
                try:
                    output = next(stack[-1])
                except StopIteration:
                    stack.pop()
                    continue
                self._assert_owned(context, remote=True)
                if isinstance(output, Request):
                    if output.seed is None:
                        output.seed = seed
                    if output.seed.seed_id != seed.seed_id:
                        raise ValueError("Request must belong to its active Seed")
                    stack.append(self._download_parse(output, context))
                    if len(stack) > self.settings["REQUEST_BUFFER_LIMIT"]:
                        raise ValueError("Nested callback depth exceeds REQUEST_BUFFER_LIMIT; use a child Seed")
                elif isinstance(output, Item):
                    context.stage = "storage"
                    if output.seed is None:
                        output.seed = seed
                    if output.seed.seed_id != seed.seed_id:
                        raise ValueError("Item must belong to its active Seed")
                    while True:
                        self._assert_owned(context, remote=True)
                        try:
                            future = self.pipeline.submit(output, timeout=0.1,
                                valid=lambda: not context.lost.is_set() and self.queue.is_owned(context.claim))
                            break
                        except TimeoutError:
                            continue
                    context.pending_items.append(future)
                    per_seed_limit = max(1, min(self.settings["ITEM_BATCH_SIZE"],
                        self.settings["ITEM_BUFFER_LIMIT"] // self.settings["SEED_CONCURRENCY"]))
                    if len(context.pending_items) >= per_seed_limit:
                        self._await_items(context, all_items=False)
                elif isinstance(output, Seed):
                    context.stage = "enqueue"
                    self.enqueue(output, parent=seed)
                else:
                    raise TypeError(f"Unsupported callback output {type(output).__name__}")
            context.stage = "storage" if context.pending_items else context.stage
            self._await_items(context)
            self._assert_owned(context)
            context.stage = "ack"
            if not self.queue.ack(context.claim):
                raise LeaseLost("ACK rejected by expired lease")
            self.metrics.counter("seed_succeeded")
        except LeaseLost:
            context.lost.set()
            self.log.event("lease_lost", level=logging.WARNING, seed_id=seed.seed_id, execution_count=seed.execution_count)
        except RedisError as exc:
            # Nothing is acknowledged. Lease recovery resumes with a persisted budget.
            context.lost.set()
            self.log.event("redis_unavailable", level=logging.ERROR, seed_id=seed.seed_id, error=safe_error(exc))
        except Exception as exc:
            original_stage = context.stage
            try:
                self._await_items(context)
            except Exception:
                # Preserve the triggering failure while counting confirmed partial writes.
                pass
            details = {"stage": original_stage, "error_type": getattr(exc, "error_type", type(exc).__name__),
                       "message": safe_error(exc), "request": context.request_summary,
                       "execution_count": seed.execution_count, "partial_summary": dict(context.partial),
                       "traceback": safe_error("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))}
            try:
                self._assert_owned(context, remote=True)
                if self.queue.fail(context.claim, details):
                    self.metrics.counter("seed_failed")
                    self.log.event("seed_failed", level=logging.ERROR, seed_id=seed.seed_id, **details)
            except LeaseLost:
                self.log.event("lease_lost", level=logging.WARNING, seed_id=seed.seed_id, execution_count=seed.execution_count)
            except RedisError as redis_error:
                context.lost.set()
                self.log.event("failure_unconfirmed", level=logging.ERROR, seed_id=seed.seed_id, error=safe_error(redis_error))
        finally:
            self._local.context = None
            for generator in locals().get("stack", ()):
                close = getattr(generator, "close", None)
                if close:
                    try:
                        close()
                    except Exception:
                        pass

    def _maintain(self):
        last_renew = last_scan = last_metrics = 0.0
        storage_healthy = True
        known_workers = set()
        previous = {}
        stalled_intervals = 0
        while not self._maintenance_stop.wait(min(self.settings["LEASE_RENEW_INTERVAL"], self.settings["RECOVERY_SCAN_INTERVAL"], 0.2)):
            now = time.monotonic()
            try:
                healthy = self.pipeline.healthy.is_set()
                if healthy != storage_healthy:
                    self.log.event("storage_restored" if healthy else "storage_unavailable", level=logging.WARNING)
                    storage_healthy = healthy
                if now - last_renew >= self.settings["LEASE_RENEW_INTERVAL"]:
                    with self._context_lock:
                        contexts = list(self._contexts.values())
                    for context in contexts:
                        if not context.lost.is_set() and not self.queue.renew(context.claim):
                            context.lost.set()
                    self.queue.heartbeat(self.worker_id, {"inflight": len(contexts), "buffer_size": self.pipeline.size,
                                                        "stopping": self._stop.is_set()}, ttl=max(3, self.settings["LEASE_TIMEOUT"]))
                    self.metrics.counter("worker_heartbeat")
                    last_renew = now
                if now - last_scan >= self.settings["RECOVERY_SCAN_INTERVAL"]:
                    recovered = self.queue.recover()
                    self.metrics.counter("seed_retried", recovered.get("reclaimed", 0))
                    self.metrics.counter("seed_failed", recovered.get("failed", 0))
                    if recovered.get("reclaimed") or recovered.get("failed"):
                        self.log.event("lease_recovery", **recovered)
                    last_scan = now
                if now - last_metrics >= self.settings["METRICS_INTERVAL"]:
                    snapshot, counts = self.metrics.snapshot(), self.queue.counts()
                    self.log.event("metrics", metrics=snapshot, queue=counts, buffer_size=self.pipeline.size)
                    counters = snapshot["counters"]
                    attempted = counters.get("request_attempt", 0) - previous.get("request_attempt", 0)
                    failed = counters.get("request_failure", 0) - previous.get("request_failure", 0)
                    if attempted >= 5 and failed >= attempted * 0.5:
                        self.log.event("high_failure_rate", level=logging.WARNING, attempts=attempted, failures=failed)
                    if counts["todo"] + counts["retry"] and counters.get("seed_succeeded", 0) == previous.get("seed_succeeded", 0):
                        stalled_intervals += 1
                        if stalled_intervals == 3:
                            self.log.event("backlog_stalled", level=logging.WARNING, queue=counts)
                    else:
                        stalled_intervals = 0
                    workers = {worker["worker_id"] for worker in self.queue.workers()}
                    for worker_id in known_workers - workers:
                        self.log.event("worker_lost", level=logging.WARNING, lost_worker_id=worker_id)
                    known_workers, previous = workers, counters
                    last_metrics = now
            except RedisError as exc:
                self.log.event("redis_unavailable", level=logging.WARNING, error=safe_error(exc))
            except Exception as exc:
                # Monitoring is best effort and cannot fail already stored data.
                self.log.event("monitor_error", level=logging.WARNING, error=safe_error(exc))

    def start(self, *, until_idle=False, idle_timeout=1.0, max_seeds=None):
        if self._running or self._contexts:
            raise ConfigurationError("This Crawler already has a running Worker")
        if not self._request_callback or "default" not in self._parse_callbacks:
            raise ConfigurationError("Register request and default parse before starting")
        if max_seeds is not None and (type(max_seeds) is not int or max_seeds < 1):
            raise ValueError("max_seeds must be positive")
        from spider_man.buffer.item_buffer import PipelineManager
        try:
            self._connect()
            retry_exceptions = tuple(import_object(path) for path in self.settings["RETRY_EXCEPTIONS"])
            if any(not isinstance(exc, type) or not issubclass(exc, Exception) for exc in retry_exceptions):
                raise ConfigurationError("RETRY_EXCEPTIONS must refer to Exception subclasses")
            self._retry_exceptions = retry_exceptions
            for hook in self._start_hooks:
                hook()
            self.pipeline = PipelineManager(self)
            self.pipeline.open()
            downloader = import_object(self.settings["DOWNLOADER"])(self)
            if not callable(getattr(downloader, "download", None)) or not callable(getattr(downloader, "close", None)):
                raise ConfigurationError("Downloader must implement download(request) and close()")
            self.downloader = downloader
        except Exception:
            try:
                self.close()
            except Exception as cleanup_error:
                self.log.event("cleanup_error", level=logging.WARNING, error=safe_error(cleanup_error))
            raise
        self._running = True
        self._stop.clear()
        self._maintenance_stop.clear()
        self.log.event("worker_started", collection=self.collection_name, config=self.settings.summary())
        jobs = thread_queue.Queue(maxsize=self.settings["SEED_CONCURRENCY"])
        done = thread_queue.Queue()

        def execute_jobs():
            while True:
                try:
                    context = jobs.get(timeout=0.1)
                except thread_queue.Empty:
                    if self._maintenance_stop.is_set():
                        return
                    continue
                if context is None:
                    return
                try:
                    self._execute(context)
                finally:
                    done.put(context.claim.token)

        threads = [threading.Thread(target=execute_jobs, daemon=True, name=f"spider-seed-{i}")
                   for i in range(self.settings["SEED_CONCURRENCY"])]
        for thread in threads:
            thread.start()
        maintainer = threading.Thread(target=self._maintain, daemon=True, name="spider-maintenance")
        maintainer.start()
        previous_signals = {}
        if threading.current_thread() is threading.main_thread():
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous_signals[sig] = signal.signal(sig, lambda signum, frame: self.stop())
        claimed, idle_since, shutdown_at = 0, None, None
        try:
            recovered = self.queue.recover()
            self.metrics.counter("seed_retried", recovered.get("reclaimed", 0))
            self.metrics.counter("seed_failed", recovered.get("failed", 0))
            while True:
                while True:
                    try:
                        token = done.get_nowait()
                    except thread_queue.Empty:
                        break
                    with self._context_lock:
                        self._contexts.pop(token, None)
                with self._context_lock:
                    active = len(self._contexts)
                if self._stop.is_set():
                    shutdown_at = shutdown_at or time.monotonic() + self.settings["SHUTDOWN_TIMEOUT"]
                    if not active or time.monotonic() >= shutdown_at:
                        break
                elif max_seeds is not None and claimed >= max_seeds:
                    self.stop()
                elif active < self.settings["SEED_CONCURRENCY"] and self.pipeline.healthy.is_set():
                    try:
                        claim = self.queue.claim(self.worker_id)
                        if claim is not None:
                            context = Execution(claim)
                            with self._context_lock:
                                self._contexts[claim.token] = context
                            jobs.put(context)
                            self.metrics.counter("seed_claimed")
                            claimed += 1
                            idle_since = None
                            continue
                        counts = self.queue.counts()
                        if until_idle and not active and not any(counts[k] for k in ("todo", "processing", "retry")):
                            idle_since = idle_since or time.monotonic()
                            if time.monotonic() - idle_since >= idle_timeout:
                                self.stop()
                        else:
                            idle_since = None
                    except RedisError as exc:
                        idle_since = None
                        self.log.event("redis_unavailable", level=logging.WARNING, error=safe_error(exc))
                # stop.wait would busy-spin during grace; sleep keeps renewal live.
                time.sleep(self.settings["POLL_INTERVAL"])
        finally:
            self._maintenance_stop.set()
            with self._context_lock:
                unfinished = list(self._contexts.values())
            for context in unfinished:
                context.lost.set()
            if unfinished:
                self.log.event("shutdown_timeout", level=logging.WARNING, recoverable_seeds=len(unfinished))
            for _ in threads:
                try:
                    jobs.put_nowait(None)
                except thread_queue.Full:
                    break
            maintainer.join(timeout=1)
            for sig, previous in previous_signals.items():
                signal.signal(sig, previous)
            self._running = False
            # Active calls may finish after grace; they cannot ACK or emit new output.
            if not unfinished:
                self.close()
            else:
                def deferred_close():
                    for thread in threads:
                        thread.join()
                    try:
                        # A storage call begun before lease loss can outlive the grace.
                        try:
                            self.pipeline.close(timeout=self.settings["SHUTDOWN_TIMEOUT"])
                        except TimeoutError:
                            self.log.event("cleanup_pending", level=logging.WARNING)
                            self.pipeline._thread.join()
                        self._close_resources()
                    except Exception as cleanup_error:
                        self.log.event("cleanup_error", level=logging.WARNING, error=safe_error(cleanup_error))
                    finally:
                        with self._context_lock:
                            self._contexts.clear()
                threading.Thread(target=deferred_close, daemon=True).start()
            self.log.event("worker_stopped", metrics=self.metrics.snapshot(), recoverable_seeds=len(unfinished))
        return self.metrics.snapshot()

    def close(self):
        if self._running or self._contexts:
            self.stop()
            return
        self._close_resources()

    def _close_resources(self):
        if self.pipeline is not None:
            self.pipeline.close(timeout=self.settings["SHUTDOWN_TIMEOUT"])
            self.pipeline = None
        if self.downloader is not None:
            self.downloader.close()
            self.downloader = None
        if self._owns_mongo and self.mongo is not None:
            self.mongo.close()
            self.mongo = None
        if self._owns_redis and self.redis is not None:
            self.redis.close()
            self.redis, self.queue = None, None


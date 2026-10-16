"""Extensible pipeline execution with bounded buffering and confirmations.

Batch pipelines return one result or exception for every input, in input order.
A batch-wide exception means none of its results can be confirmed. Retrying an
unconfirmed insert can produce duplicates when no business unique key exists.
"""

from __future__ import annotations

import copy
import importlib
import logging
import queue
import threading
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Any

from spider_man.exceptions import ConfigurationError, DropItem, ItemValidationError, LeaseLost
from spider_man.pipelines._storage import _recoverable, _storage_error
from spider_man.pipelines.base_pipeline import PipelineBatchError, PipelineResult

log = logging.getLogger(__name__)


@dataclass
class _Submission:
    item: Any
    future: Future
    stage: int = 0
    outcome: PipelineResult | None = None
    submitted_at: float = field(default_factory=time.monotonic)
    completed: bool = False
    valid: Any = None


class PipelineManager:
    """Run a chain with bounded outstanding items and confirmed Future results."""

    def __init__(self, app, pipelines=None):
        self.app = app
        settings = app.settings
        self.limit = settings.get("ITEM_BUFFER_LIMIT", 2000)
        self.batch_size = settings.get("ITEM_BATCH_SIZE", 100)
        self.interval = settings.get("ITEM_FLUSH_INTERVAL", 1.0)
        self.max_retries = settings.get("ITEM_MAX_RETRIES", 3)
        self.retry_timeout = settings.get("ITEM_RETRY_TIMEOUT", 300.0)
        self.backoff = settings.get("ITEM_RETRY_BACKOFF", 0.1)
        self.max_backoff = settings.get("ITEM_RETRY_BACKOFF_MAX", 5.0)
        self.pipelines = list(pipelines) if pipelines is not None else self._load_pipelines(settings)
        self.healthy = threading.Event()
        self.healthy.set()
        self._capacity = threading.BoundedSemaphore(self.limit)
        self._queue = queue.Queue(maxsize=self.limit)
        self._condition = threading.Condition()
        self._outstanding = 0
        self._closing = threading.Event()
        self._abort = threading.Event()
        self._wake = threading.Event()
        self._thread = None
        self._opened = []
        self._last_probe = 0.0
        self._unhealthy = set()

    def _load_pipelines(self, settings):
        configured = settings.get("ITEM_PIPELINES", ["spider_man.pipelines.mongo_pipeline.MongoPipeline"])
        if isinstance(configured, str) or not isinstance(configured, (list, tuple)):
            raise ConfigurationError("ITEM_PIPELINES must be a list of class paths")
        instances = []
        for spec in configured:
            if isinstance(spec, str):
                module, _, name = spec.rpartition(".")
                if not module or not name:
                    raise ConfigurationError("Pipeline class paths must include a module and class name")
                try:
                    factory = getattr(importlib.import_module(module), name)
                except (ImportError, AttributeError) as error:
                    raise ConfigurationError(f"Cannot import Pipeline {spec!r}") from error
                instances.append(factory(self.app))
            elif isinstance(spec, type):
                instances.append(spec(self.app))
            else:
                raise ConfigurationError("ITEM_PIPELINES entries must be class paths or classes")
        return instances

    @property
    def size(self):
        with self._condition:
            return self._outstanding

    @property
    def buffer_size(self):
        return self.size

    def open(self):
        if self._thread is not None:
            raise RuntimeError("PipelineManager is already open")
        if not any(getattr(pipeline, "required", True) and getattr(pipeline, "persistent", False) for pipeline in self.pipelines):
            if not self.app.settings.get("ALLOW_CONSOLE_ONLY", False):
                raise ConfigurationError("At least one required persistent Pipeline is needed; ALLOW_CONSOLE_ONLY explicitly enables local debugging")
        try:
            for pipeline in self.pipelines:
                if not callable(getattr(pipeline, "process_item", None)) and not callable(getattr(pipeline, "process_batch", None)):
                    raise ConfigurationError("A Pipeline must define process_item or process_batch")
                hook = getattr(pipeline, "open", None)
                if hook:
                    hook()
                self._opened.append(pipeline)
        except Exception:
            for pipeline in reversed(self._opened):
                hook = getattr(pipeline, "close", None)
                if hook:
                    try:
                        hook()
                    except Exception:
                        log.exception("Pipeline cleanup failed after startup error")
            self._opened.clear()
            raise
        self._thread = threading.Thread(target=self._run, name="spider-man-items", daemon=True)
        self._thread.start()

    def submit(self, item, timeout=None, valid=None):
        if self._thread is None:
            raise RuntimeError("PipelineManager is not open")
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._closing.is_set():
            wait = 0.1 if deadline is None else max(0, min(0.1, deadline - time.monotonic()))
            if self._capacity.acquire(timeout=wait):
                with self._condition:
                    if self._closing.is_set():
                        self._capacity.release()
                        break
                    future = Future()
                    future.set_running_or_notify_cancel()
                    # Freeze the submitted business payload while retaining Seed identity.
                    try:
                        from spider_man.network.item import Item
                        frozen = Item(data=copy.deepcopy(item.data), seed=item.seed)
                    except Exception:
                        self._capacity.release()
                        raise
                    self._outstanding += 1
                    self._queue.put_nowait(_Submission(frozen, future, valid=valid))
                    self._condition.notify_all()
                    return future
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Pipeline item buffer is full")
        raise RuntimeError("PipelineManager is closing")

    def _counter(self, name):
        try:
            self.app.metrics.counter(name, 1)
        except Exception:
            log.exception("Item metrics failed")

    def _finish(self, submission, result=None, error=None):
        if submission.completed:
            return
        submission.completed = True
        if error is None:
            result = result or PipelineResult("unchanged")
            self._counter("item_" + result.status)
            if not submission.future.cancelled():
                submission.future.set_result(result)
        else:
            self._counter("item_failed")
            if not submission.future.cancelled():
                submission.future.set_exception(error)
        with self._condition:
            self._outstanding -= 1
            self._capacity.release()
            self._condition.notify_all()

    def _interpret(self, pipeline, submission, value):
        if isinstance(value, Exception):
            raise value
        if isinstance(value, PipelineResult):
            if value.status == "dropped":
                return value
            if getattr(pipeline, "persistent", False):
                submission.outcome = value
            return None
        if getattr(pipeline, "persistent", False):
            raise ConfigurationError("Persistent Pipeline must return an explicit per-item PipelineResult")
        if value is None:
            return None
        if isinstance(value, Mapping):
            from spider_man.network.item import Item
            submission.item = Item(data=dict(value), seed=submission.item.seed)
            return None
        if hasattr(value, "data") and hasattr(value, "seed"):
            old_seed = getattr(submission.item.seed, "seed_id", None)
            new_seed = getattr(value.seed, "seed_id", None)
            if value.seed is not submission.item.seed and (old_seed is None or old_seed != new_seed):
                raise ItemValidationError("Pipeline transforms must preserve the originating Seed")
            submission.item = value
            return None
        raise ConfigurationError("Pipeline result must be an Item, mapping, PipelineResult, or None")

    def _invoke(self, pipeline, submissions):
        items = [submission.item for submission in submissions]
        batch_method = getattr(pipeline, "process_batch", None)
        if batch_method:
            try:
                values = batch_method(items)
            except PipelineBatchError as error:
                values = error.results
            except Exception as error:
                return [error] * len(items)
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or len(values) != len(items):
                error = ConfigurationError("process_batch must return one result or exception for each input Item")
                return [error] * len(items)
            return list(values)
        results = []
        for index, item in enumerate(items):
            try:
                results.append(pipeline.process_item(item))
            except Exception as error:
                results.append(error)
                if _recoverable(error):
                    results.extend([error] * (len(items) - index - 1))
                    break
        return results

    def _process(self, submissions):
        active = submissions
        started = time.monotonic()
        for stage, pipeline in enumerate(self.pipelines):
            if not active:
                break
            pending = list(active)
            advanced = []
            attempts = {id(submission): 0 for submission in pending}
            had_failure = False
            while pending:
                if self._abort.is_set():
                    for submission in pending:
                        self._finish(submission, error=_storage_error("Storage interrupted during shutdown", retryable=True))
                    pending = []
                    break
                valid_pending = []
                for submission in pending:
                    try:
                        if submission.valid is not None and not submission.valid():
                            raise LeaseLost("Item source Seed lease was lost before storage started")
                        valid_pending.append(submission)
                    except Exception as error:
                        self._finish(submission, error=error)
                pending = valid_pending
                if not pending:
                    break
                results = self._invoke(pipeline, pending)
                retry = []
                for submission, value in zip(pending, results):
                    try:
                        dropped = self._interpret(pipeline, submission, value)
                        if dropped:
                            self._finish(submission, result=dropped)
                        else:
                            submission.stage = stage + 1
                            advanced.append(submission)
                    except DropItem as error:
                        reason = getattr(error, "reason", None) or str(error)
                        try:
                            self._finish(submission, result=PipelineResult("dropped", reason))
                        except ValueError as invalid_reason:
                            self._finish(submission, error=ItemValidationError(str(invalid_reason)))
                    except Exception as error:
                        if _recoverable(error):
                            had_failure = True
                            self._unhealthy.add(id(pipeline))
                            self.healthy.clear()
                            elapsed = time.monotonic() - started
                            if attempts[id(submission)] < self.max_retries and elapsed < self.retry_timeout:
                                attempts[id(submission)] += 1
                                retry.append(submission)
                                continue
                            message = "Storage retry timeout exhausted" if elapsed >= self.retry_timeout else "Storage retry budget exhausted"
                            exhausted = _storage_error(message, retryable=True)
                            exhausted.__cause__ = error
                            self._finish(submission, error=exhausted)
                        else:
                            self._finish(submission, error=error)
                if retry:
                    count = max(attempts[id(submission)] for submission in retry)
                    delay = min(self.max_backoff, self.backoff * (2 ** (count - 1)),
                                max(0, self.retry_timeout - (time.monotonic() - started)))
                    self._abort.wait(delay)
                    if time.monotonic() - started >= self.retry_timeout:
                        for submission in retry:
                            self._finish(submission, error=_storage_error("Storage retry timeout exhausted", retryable=True))
                        retry = []
                pending = retry
            if advanced and (not had_failure or len(advanced) == len(active)):
                self._unhealthy.discard(id(pipeline))
                if not self._unhealthy:
                    self.healthy.set()
            active = advanced
        for submission in active:
            self._finish(submission, result=submission.outcome)
        self._probe_health()

    def _probe_health(self):
        if self.healthy.is_set() or time.monotonic() - self._last_probe < min(self.interval, 1.0):
            return
        self._last_probe = time.monotonic()
        checks = [getattr(pipeline, "check_health", None) for pipeline in self.pipelines if id(pipeline) in self._unhealthy]
        if not checks or any(check is None for check in checks):
            # Generic plugins without a health hook recover upon their next confirmed write.
            return
        try:
            for check in checks:
                check()
        except Exception:
            return
        self._unhealthy.clear()
        self.healthy.set()

    def _run(self):
        while not self._closing.is_set() or not self._queue.empty():
            try:
                first = self._queue.get(timeout=min(self.interval, 0.1))
            except queue.Empty:
                self._probe_health()
                continue
            submissions = [first]
            deadline = time.monotonic() + self.interval
            while len(submissions) < self.batch_size:
                try:
                    submissions.append(self._queue.get_nowait())
                    continue
                except queue.Empty:
                    pass
                if self._closing.is_set() or self._wake.is_set():
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    submissions.append(self._queue.get(timeout=min(remaining, 0.05)))
                except queue.Empty:
                    pass
            try:
                self._process(submissions)
            except Exception as error:
                log.exception("Unexpected Pipeline manager error")
                # Only uncompleted submissions remain eligible for failure.
                for submission in submissions:
                    if not submission.completed:
                        self._finish(submission, error=error)
            finally:
                for _ in submissions:
                    self._queue.task_done()
                if self.size == 0:
                    self._wake.clear()

    def flush(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        self._wake.set()
        with self._condition:
            while self._outstanding:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("Pipeline flush timed out with unconfirmed items")
                self._condition.wait(remaining)
        for pipeline in self._opened:
            hook = getattr(pipeline, "flush", None)
            if hook:
                hook()

    def close(self, timeout=None):
        self._closing.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout)
            if self._thread.is_alive():
                self._abort.set()
                raise TimeoutError("Pipeline shutdown timed out with unconfirmed items")
        for pipeline in reversed(self._opened):
            hook = getattr(pipeline, "flush", None)
            if hook:
                hook()
            hook = getattr(pipeline, "close", None)
            if hook:
                hook()
        self._opened.clear()

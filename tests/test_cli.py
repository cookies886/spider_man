"""CLI contracts using real task objects and an isolated real Redis queue."""

import json
import sys
from types import ModuleType
import uuid

import mongomock
import pytest

from spider_man import Crawler
from spider_man.base import Seed
from spider_man.commands import cmdline as cli
from spider_man.db.mongodb import MongoDB


@pytest.fixture
def task_factory(redis_client, monkeypatch):
    tasks = []

    def create(**settings):
        mongo = MongoDB(client=mongomock.MongoClient(), db="cli_test")
        app = Crawler("cli_test", "posts", redis=redis_client, mongo=mongo, custom_settings={
            "REDIS_URL": "redis://localhost:6379/15",
            "MONGO_URI": "mongodb://localhost:27017",
            "MONGO_DATABASE": "cli_test",
            "LOG_LEVEL": "CRITICAL",
            "POLL_INTERVAL": 0.01,
            **settings,
        })
        name = "_cli_task_" + uuid.uuid4().hex
        module = ModuleType(name)
        module.app = app
        monkeypatch.setitem(sys.modules, name, module)
        tasks.append((app, mongo))
        return app, name + ":app"

    yield create
    for app, mongo in tasks:
        app.close()
        mongo.close()


def _invoke(capsys, *arguments, expected_exit=0):
    assert cli.main(list(arguments)) == expected_exit
    captured = capsys.readouterr()
    if expected_exit == 0:
        assert captured.err == ""
        return json.loads(captured.out)
    assert captured.out == ""
    return json.loads(captured.err)


def test_jsonl_seed_reports_accepted_duplicate_and_rejected_lines(task_factory, tmp_path, capsys):
    app, reference = task_factory()
    envelope = Seed({"user_id": "envelope"}, seed_id="stable-input")
    unsupported = json.loads(envelope.to_json())
    unsupported["version"] = 2
    seed_file = tmp_path / "seeds.jsonl"
    seed_file.write_text("\n".join([
        "", '{"user_id":"business"}', envelope.to_json(), envelope.to_json(),
        '{"invalid_json":', "[1, 2]", json.dumps(unsupported), '{"user_id":"business"}',
    ]) + "\n", encoding="utf-8")

    result = _invoke(capsys, "seed", reference, str(seed_file))
    assert {key: result[key] for key in ("accepted", "duplicate", "rejected")} == {
        "accepted": 3, "duplicate": 1, "rejected": 3,
    }
    assert [error["line"] for error in result["errors"]] == [5, 6, 7]
    # Raw business dictionaries are distinct logical tasks; explicit envelope
    # identity is the deduplication contract, not a hash of business content.
    assert app.queue.counts() == {"todo": 3, "processing": 0, "retry": 0, "failed": 0}
    assert app.queue.claim("reader").seed.execution_count == 1


def _record_failures(app):
    app._connect_redis()
    first = Seed({"user_id": "timeout"}, priority=1)
    second = Seed({"user_id": "invalid"}, priority=2)
    app.enqueue(first)
    app.enqueue(second)
    assert app.queue.fail(app.queue.claim("worker"), {
        "stage": "download", "error_type": "timeout", "message": "timed out",
        "partial_summary": {"items_confirmed": 1},
    })
    assert app.queue.fail(app.queue.claim("worker"), {
        "stage": "parse", "error_type": "TypeError", "message": "invalid body",
    })
    return first, second


def test_status_failed_and_filtered_retry_keep_original_failure_records(task_factory, capsys):
    app, reference = task_factory()
    first, second = _record_failures(app)
    app.queue.heartbeat("worker", {"inflight": 0}, ttl=30)

    status = _invoke(capsys, "status", reference)
    assert status["project"] == "cli_test" and status["task"] == "posts"
    assert status["queue"] == {"todo": 0, "processing": 0, "retry": 0, "failed": 2}
    assert status["workers"][0]["worker_id"] == "worker"
    assert {failure["seed_id"] for failure in status["recent_errors"]} == {first.seed_id, second.seed_id}
    failures = _invoke(capsys, "failed", reference, "--error-type", "timeout", "--limit", "1")
    assert len(failures) == 1 and failures[0]["seed_id"] == first.seed_id
    assert failures[0]["stage"] == "download" and failures[0]["execution_count"] == 1

    replay = _invoke(capsys, "retry", reference, "--seed-id", first.seed_id, "--limit", "1")
    assert replay["replayed"] == 1 and replay["original_failures_retained"] is True
    assert replay["records"][0]["original_seed_id"] == first.seed_id
    replay_id = replay["records"][0]["replay_seed_id"]
    assert replay_id != first.seed_id
    assert replay["dedup_window_seconds"] == app.settings["DEDUP_TTL"]
    assert app.queue.counts() == {"todo": 1, "processing": 0, "retry": 0, "failed": 2}
    original = _invoke(capsys, "failed", reference, "--seed-id", first.seed_id)
    assert original[0]["seed_id"] == first.seed_id
    assert original[0]["partial_summary"] == {"items_confirmed": 1}
    assert original[0]["replays"][0]["seed_id"] == replay_id
    claim = app.queue.claim("replay-worker")
    assert claim.seed.seed_id == replay_id and claim.seed.execution_count == 1
    assert claim.seed.metadata["replay_of"] == first.seed_id


@pytest.mark.parametrize("command", ["failed", "retry"])
@pytest.mark.parametrize("limit", [0, 1001])
def test_failed_and_retry_reject_unbounded_or_empty_batches(task_factory, capsys, command, limit):
    app, reference = task_factory()
    _record_failures(app)
    error = _invoke(capsys, command, reference, "--limit", str(limit), expected_exit=1)
    assert error["error_type"] == "ConfigurationError"
    assert "--limit" in error["error"]
    assert app.queue.counts() == {"todo": 0, "processing": 0, "retry": 0, "failed": 2}


def test_check_is_readonly_and_reports_pending_startup_hooks(task_factory, capsys):
    app, reference = task_factory()
    hooks = []

    @app.on_start
    def startup():
        hooks.append("called")
        app.mongo.create_index(app.collection_name, ["business_id"], unique=True)

    seed = Seed({"user_id": "pending"})
    app.enqueue(seed)
    result = _invoke(capsys, "check", reference)
    assert result["ready"] is True
    assert result["startup_hooks_pending"] == 1
    assert result["queue"] == {"todo": 1, "processing": 0, "retry": 0, "failed": 0}
    assert hooks == [] and app.mongo.count(app.collection_name) == 0
    assert "business_id_1" not in app.mongo.get_index(app.collection_name)
    assert app.queue.executions(seed.seed_id) == []


def test_check_reports_missing_unique_index_without_running_creation_hook(task_factory, capsys):
    app, reference = task_factory(MONGO_UPSERT_KEYS=["business_id"])
    hooks = []

    @app.on_start
    def startup():
        hooks.append("called")
        app.mongo.create_index(app.collection_name, ["business_id"], unique=True)

    # check readiness failure still prints a complete diagnostic report to stdout.
    assert cli.main(["check", reference]) == 1
    captured = capsys.readouterr()
    assert captured.err == ""
    result = json.loads(captured.out)
    assert result["ready"] is False and result["startup_hooks_pending"] == 1
    assert "unique" in result["error"].lower() and hooks == []
    assert app.queue.counts()["processing"] == 0


def test_run_applies_cli_overrides_and_preserves_environment_sources(task_factory, monkeypatch, capsys):
    monkeypatch.setenv("REQUEST_TIMEOUT", "17")
    monkeypatch.setenv("SEED_CONCURRENCY", "5")
    app, reference = task_factory(REQUEST_CONCURRENCY=7, REQUEST_TIMEOUT=3)

    @app.request
    def request(seed):
        return None

    @app.parse
    def parse(response):
        return None

    app.enqueue(Seed({"user_id": "one"}))
    result = _invoke(capsys, "run", reference, "--seed-concurrency", "2",
                     "--request-concurrency", "3", "--max-seeds", "1")
    assert result["counters"]["seed_succeeded"] == 1
    summary = app.settings.summary()
    assert summary["SEED_CONCURRENCY"] == {"value": 5, "source": "environment"}
    assert summary["REQUEST_CONCURRENCY"] == {"value": 3, "source": "cli"}
    assert summary["REQUEST_TIMEOUT"] == {"value": 17, "source": "environment"}
    assert summary["ITEM_BATCH_SIZE"]["source"] == "default"
    assert summary["LOG_LEVEL"]["source"] == "task"


def test_missing_connection_config_reports_error_without_consuming_seed(task_factory, capsys):
    app, reference = task_factory(MONGO_DATABASE=None)
    seed = Seed({"user_id": "pending"})
    app.enqueue(seed)
    error = _invoke(capsys, "check", reference, expected_exit=1)
    assert error["error_type"] == "ConfigurationError"
    assert "MONGO_DATABASE" in error["error"]
    assert app.queue.counts() == {"todo": 1, "processing": 0, "retry": 0, "failed": 0}
    assert app.queue.executions(seed.seed_id) == []


def test_task_file_reference_loads_real_crawler(task_factory, tmp_path, capsys):
    app, reference = task_factory()
    module_name = reference.split(":")[0]
    task_file = tmp_path / "task.py"
    task_file.write_text(f"from {module_name} import app\n", encoding="utf-8")
    app.enqueue(Seed({"user_id": "one"}))
    result = _invoke(capsys, "status", str(task_file) + ":app")
    assert result["queue"]["todo"] == 1


def test_invalid_crawler_reference_returns_actionable_error(capsys):
    error = _invoke(capsys, "status", "missing-colon", expected_exit=1)
    assert error["error_type"] == "ConfigurationError"
    assert "module:app" in error["error"]


@pytest.mark.parametrize("setting", ["DOWNLOADER", "PROXY_PROVIDER", "ITEM_PIPELINES"])
def test_check_rejects_missing_configured_extension_without_consuming(task_factory, capsys, setting):
    path = "no_such_spider_man_extension.Extension"
    app, reference = task_factory(**{setting: [path] if setting == "ITEM_PIPELINES" else path})
    seed = Seed({"user_id": "pending"})
    app.enqueue(seed)
    assert cli.main(["check", reference]) == 1
    captured = capsys.readouterr()
    diagnostic = json.loads(captured.err or captured.out)
    assert diagnostic.get("ready") is False or diagnostic["error_type"] == "ConfigurationError"
    assert "no_such_spider_man_extension" in diagnostic["error"]
    assert app.queue.counts() == {"todo": 1, "processing": 0, "retry": 0, "failed": 0}
    assert app.queue.executions(seed.seed_id) == []

import json
import traceback
import pytest

from spider_man import Item, Crawler, Request, Seed
from spider_man.config import Settings
from spider_man.exceptions import ConfigurationError


def test_seed_protocol_preserves_business_and_controls():
    data = {"seed_id": "business-id", "priority": "business-value", "video": 9007199254740993123, "empty": []}
    seed = Seed(data, priority=1, metadata={"source": "test"})
    restored = Seed.from_json(seed.to_json())
    assert restored.to_dict() == data
    assert restored.seed_id != restored["seed_id"]
    assert restored.params.priority == 1 and restored.video == data["video"]
    assert restored.page is None


def test_stable_children_are_scoped_to_parent_and_target():
    parent = Seed({"user": 1})
    first = Seed({"page": 2}).derive_id(parent, "x", "list")
    repeated = Seed({"page": 2}).derive_id(parent, "x", "list")
    other = Seed({"page": 2}).derive_id(parent, "x", "detail")
    assert first.seed_id == repeated.seed_id != other.seed_id
    assert Seed({"page": 2}).derive_id(Seed({"user": 1}), "x", "list").seed_id != first.seed_id


def test_configuration_precedence_snapshot_and_sources():
    configured = Settings({"SEED_CONCURRENCY": 3}, {"SEED_CONCURRENCY": 5},
                          {"SEED_CONCURRENCY": "4"})
    assert configured.SEED_CONCURRENCY == 4
    assert configured.sources["SEED_CONCURRENCY"] == "environment"
    env = Settings({"SEED_CONCURRENCY": 3}, environ={"SEED_CONCURRENCY": "4"})
    assert env.SEED_CONCURRENCY == 4
    assert env.with_cli({"REQUEST_CONCURRENCY": 1}).sources["SEED_CONCURRENCY"] == "environment"
    settings = Settings(custom_settings={"DEFAULT_PROXY": {"http": "http://user:secret@localhost:8080"}}, environ={})
    with pytest.raises(TypeError):
        settings.DEFAULT_PROXY["http"] = "changed"
    assert "secret" not in json.dumps(settings.summary())
    assert Settings(environ={}).DEFAULT_PROXY is None


@pytest.mark.parametrize("setting,value", [("SEED_CONCURRENCY", True), ("SEED_CONCURRENCY", 0),
    ("REQUEST_MAX_RETRIES", -1), ("LEASE_RENEW_INTERVAL", 120), ("REQUEST_TIMEOUT", float("nan")),
    ("DEFAULT_PROXY", True), ("DEFAULT_PROXY", 1), ("DEFAULT_PROXY", {"http": 2}),
    ("ITEM_PIPELINES", "spider_man.pipelines.mongo_pipeline.MongoPipeline"), ("MONGO_UPSERT_KEYS", ["a", "a"]),
    ("DEDUP_TTL", 1), ("LOG_JSON", 1)])
def test_invalid_config_rejected_before_consumption(setting, value):
    with pytest.raises(ConfigurationError):
        Settings(custom_settings={setting: value}, environ={})


def test_invalid_names_and_legacy_settings_rejected():
    with pytest.raises(ConfigurationError):
        Settings(custom_settings={"lowercase": 1}, environ={})
    with pytest.raises(ConfigurationError):
        Settings(environ={"SPIDER_MAN_RUN_CONFIG": "{}"})


@pytest.mark.parametrize("value", ["business.settings", ["SEED_CONCURRENCY"], 1])
def test_custom_settings_requires_runtime_mapping(value):
    with pytest.raises(ConfigurationError, match="custom_settings must be a mapping"):
        Settings(custom_settings=value, environ={})


@pytest.mark.parametrize("options", [{"url": "ftp://x"}, {"url": "http://x", "callback": lambda r: r},
    {"url": "http://x", "timeout": (1,)}, {"url": "http://x", "max_retries": -1}])
def test_request_validation(options):
    with pytest.raises(ValueError):
        Request(**options)


def test_business_callbacks_are_named_and_registered():
    app = Crawler("bilibili", "user_post")
    assert app.todo_key == "{bilibili:user_post}:todo"
    assert app.collection_name == "bilibili_user_post"
    @app.parse("detail")
    def detail(response):
        return None
    assert app._parse_callbacks["detail"] is detail
    with pytest.raises(ConfigurationError):
        app.parse(detail, name="detail")


@pytest.mark.parametrize("source,topic,expected", [
    (None, None, "queue_project_queue_task"),
    ("custom_source", None, "custom_source_queue_task"),
    (None, "custom_topic", "queue_project_custom_topic"),
    ("custom_source", "custom_topic", "custom_source_custom_topic"),
])
def test_collection_names_fall_back_per_component_without_changing_queue(monkeypatch, source, topic, expected):
    monkeypatch.delenv("LOG_SOURCE", raising=False)
    monkeypatch.delenv("LOG_TOPIC", raising=False)
    app = Crawler("queue_project", "queue_task", custom_settings={"LOG_SOURCE": source, "LOG_TOPIC": topic})
    try:
        assert app.collection_name == expected
        assert app.todo_key == "{queue_project:queue_task}:todo"
        assert (app.project, app.task) == ("queue_project", "queue_task")
    finally:
        app.close()


@pytest.mark.parametrize("name", ["LOG_SOURCE", "LOG_TOPIC"])
@pytest.mark.parametrize("value", ["", " \t\n", 1, False, ["synthetic-sensitive-name"],
                                   {"secret": "synthetic-sensitive-name"}])
def test_invalid_collection_component_fails_before_connecting_without_exposing_value(monkeypatch, name, value):
    opened = []

    def unexpected_connection(*args, **kwargs):
        opened.append(True)
        raise AssertionError("Invalid settings must fail before opening a database")

    monkeypatch.delenv("LOG_SOURCE", raising=False)
    monkeypatch.delenv("LOG_TOPIC", raising=False)
    monkeypatch.setattr("spider_man.db.redisdb.RedisDB", unexpected_connection)
    monkeypatch.setattr("spider_man.db.mongodb.MongoDB", unexpected_connection)
    with pytest.raises(ConfigurationError) as captured:
        Crawler("queue_project", "queue_task", custom_settings={name: value})
    error = captured.value
    formatted = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert "synthetic-sensitive-name" not in str(error) and "synthetic-sensitive-name" not in formatted
    assert opened == []


@pytest.mark.parametrize("name", ["LOG_SOURCE", "LOG_TOPIC"])
@pytest.mark.parametrize("value", ["", " \t\n"])
def test_invalid_collection_environment_fails_during_configuration(name, value):
    with pytest.raises(ConfigurationError):
        Settings(environ={name: value})


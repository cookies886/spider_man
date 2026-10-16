"""环境配置优先级、业务类型转换、实例快照及敏感值保护。"""
from collections.abc import Mapping
import ast
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback

import pytest

from spider_man import Crawler
from spider_man.config import Settings
from spider_man.exceptions import ConfigurationError


SENSITIVE_TEST_VALUE = "synthetic-vault-value-6b7241"


@pytest.mark.parametrize("task,cli,env,expected,source", [
    (None, None, {}, 8, "default"),
    ({"SEED_CONCURRENCY": 2}, None, {}, 2, "task"),
    ({"SEED_CONCURRENCY": 3}, {"SEED_CONCURRENCY": 4}, {}, 4, "cli"),
    ({"SEED_CONCURRENCY": 3}, {"SEED_CONCURRENCY": 4}, {"SEED_CONCURRENCY": "6"}, 6, "environment"),
    ({"SEED_CONCURRENCY": 3}, {"SEED_CONCURRENCY": 4},
     {"SPIDER_MAN_SEED_CONCURRENCY": "99", "SEED_CONCURRENCY": "6"}, 6, "environment"),
    ({"SEED_CONCURRENCY": 2}, None, {"SPIDER_MAN_SEED_CONCURRENCY": "99"}, 2, "task"),
])
def test_environment_overrides_cli_task_and_defaults(task, cli, env, expected, source):
    settings = Settings(task, cli, environ=env)
    assert settings.SEED_CONCURRENCY == expected
    assert settings.sources["SEED_CONCURRENCY"] == source


def test_unprefixed_builtin_environment_uses_declared_types():
    settings = Settings(environ={
        "REQUEST_CONCURRENCY": "3", "REQUEST_INTERVAL": "0.125", "LOG_JSON": "false",
        "RETRY_HTTP_STATUS_CODES": "[429,503]", "MONGO_UPSERT_KEYS": '["aweme_id"]',
    })
    assert settings.REQUEST_CONCURRENCY == 3
    assert settings.REQUEST_INTERVAL == 0.125
    assert settings.LOG_JSON is False
    assert settings.RETRY_HTTP_STATUS_CODES == (429, 503)
    assert settings.MONGO_UPSERT_KEYS == ("aweme_id",)


def test_prefixed_names_are_plain_undeclared_environment_without_config_fallback():
    settings = Settings({"SEED_CONCURRENCY": 2, "BUSINESS_NOTE": "task"},
                        cli={"SEED_CONCURRENCY": 4}, environ={
                            "SPIDER_MAN_SEED_CONCURRENCY": "99",
                            "SPIDER_MAN_REQUEST_CONCURRENCY": "invalid-but-ignored",
                            "SPIDER_MAN_BUSINESS_NOTE": "ignored-note",
                        })
    assert settings.SEED_CONCURRENCY == 4 and settings.sources["SEED_CONCURRENCY"] == "cli"
    assert settings.REQUEST_CONCURRENCY == 16 and settings.sources["REQUEST_CONCURRENCY"] == "default"
    assert settings.BUSINESS_NOTE == "task" and settings.sources["BUSINESS_NOTE"] == "task"
    assert settings.getenv("SEED_CONCURRENCY") is None
    assert settings.getenv("SPIDER_MAN_SEED_CONCURRENCY", cast=int) == 99
    for key in ("SPIDER_MAN_SEED_CONCURRENCY", "SPIDER_MAN_REQUEST_CONCURRENCY", "SPIDER_MAN_BUSINESS_NOTE"):
        assert key not in settings and key not in settings.summary()


@pytest.mark.parametrize("env,expected,source", [
    ({"SPIDER_THREAD_NUM": "11"}, 11, "environment"),
    ({"SPIDER_THREAD_NUM": "11", "SEED_CONCURRENCY": "4"}, 4, "environment"),
    ({"SPIDER_THREAD_NUM": "11", "SPIDER_MAN_SEED_CONCURRENCY": "5"}, 11, "environment"),
    ({"SPIDER_THREAD_NUM": "invalid-but-shadowed", "SEED_CONCURRENCY": "4"}, 4, "environment"),
    ({"SPIDER_MAN_SPIDER_THREAD_NUM": "11"}, 8, "default"),
])
def test_cobweb_thread_alias_only_changes_seed_concurrency_and_loses_to_canonical_names(env, expected, source):
    settings = Settings(environ=env)
    assert settings.SEED_CONCURRENCY == expected
    assert settings.REQUEST_CONCURRENCY == 16
    assert settings.sources["SEED_CONCURRENCY"] == source
    assert "SPIDER_THREAD_NUM" not in settings


def test_with_cli_retains_environment_snapshot_without_rereading_process_environment(monkeypatch):
    environment = {"SEED_CONCURRENCY": "7", "TASK_API_KEY": SENSITIVE_TEST_VALUE}
    settings = Settings({"BUSINESS_COUNT": 1}, cli={"SEED_CONCURRENCY": 3}, environ=environment)
    environment["SEED_CONCURRENCY"] = "21"
    environment["TASK_API_KEY"] = "changed-original-mapping"
    monkeypatch.setenv("SEED_CONCURRENCY", "31")
    monkeypatch.setenv("REQUEST_CONCURRENCY", "32")
    monkeypatch.setenv("TASK_API_KEY", "changed-process-environment")

    updated = settings.with_cli({"SEED_CONCURRENCY": 4, "REQUEST_CONCURRENCY": 2})
    assert settings.SEED_CONCURRENCY == updated.SEED_CONCURRENCY == 7
    assert settings.REQUEST_CONCURRENCY == 16 and updated.REQUEST_CONCURRENCY == 2
    assert updated.sources["SEED_CONCURRENCY"] == "environment"
    assert updated.sources["REQUEST_CONCURRENCY"] == "cli"
    assert updated.sources["BUSINESS_COUNT"] == "task"
    assert settings.getenv("TASK_API_KEY") == updated.getenv("TASK_API_KEY") == SENSITIVE_TEST_VALUE


def test_with_cli_retains_alias_and_typed_business_values_from_its_snapshot():
    settings = Settings({"BUSINESS_LABELS": ["initial"]}, environ={
        "SPIDER_THREAD_NUM": "9", "BUSINESS_LABELS": '["saved","snapshot"]',
    })
    updated = settings.with_cli({"SEED_CONCURRENCY": 2})
    assert updated.SEED_CONCURRENCY == 9 and updated.sources["SEED_CONCURRENCY"] == "environment"
    assert updated.BUSINESS_LABELS == ("saved", "snapshot")
    assert updated.sources["BUSINESS_LABELS"] == "environment"


@pytest.mark.parametrize("default,raw,expected", [
    (1, "42", 42),
    (0.5, "1.25", 1.25),
    (False, "TRUE", True),
    (True, "0", False),
    ("default", "42", "42"),
    ("default", "null", "null"),
    ("default", "", ""),
    (["default"], '["one",2]', ("one", 2)),
    (("default",), '["one",2]', ("one", 2)),
    ({"default": 1}, '{"label":"two","count":2}', {"label": "two", "count": 2}),
    (None, "42", "42"),
    (None, "true", "true"),
    (None, '["one"]', '["one"]'),
    (None, "null", None),
])
def test_declared_business_environment_is_converted_from_its_final_default_type(default, raw, expected):
    settings = Settings({"BUSINESS_OPTION": default}, environ={"BUSINESS_OPTION": raw})
    assert settings.BUSINESS_OPTION == expected
    assert settings.sources["BUSINESS_OPTION"] == "environment"
    assert settings.summary()["BUSINESS_OPTION"] == {"value": "[redacted]", "source": "environment"}


def test_custom_business_declaration_controls_environment_type():
    settings = Settings({"BUSINESS_NUMBER": 0.5, "TASK_ONLY": ["task"]},
                        environ={"BUSINESS_NUMBER": "2.25"})
    assert settings.BUSINESS_NUMBER == 2.25 and type(settings.BUSINESS_NUMBER) is float
    assert settings.sources["BUSINESS_NUMBER"] == "environment"
    assert settings.TASK_ONLY == ("task",) and settings.sources["TASK_ONLY"] == "task"


def test_custom_business_containers_are_immutable_and_not_shared_with_caller():
    declared = {"BUSINESS_LABELS": ["original"], "BUSINESS_OPTIONS": {"nested": [1]}}
    settings = Settings(declared, environ={})
    declared["BUSINESS_LABELS"].append("changed")
    declared["BUSINESS_OPTIONS"]["nested"].append(2)
    assert settings.BUSINESS_LABELS == ("original",)
    assert settings.BUSINESS_OPTIONS["nested"] == (1,)
    assert isinstance(settings.BUSINESS_OPTIONS, Mapping)
    with pytest.raises(TypeError):
        settings.BUSINESS_OPTIONS["new"] = 3


def test_environment_is_not_automatically_exposed_in_mapping_or_summary():
    environment = {"TASK_API_KEY": SENSITIVE_TEST_VALUE, "PATH": "/test/bin", "UNDECLARED_TOKEN": "synthetic-token"}
    settings = Settings({"BUSINESS_HOST": "example.invalid"}, environ=environment)
    for key in environment:
        assert key not in settings and key not in settings.summary()
    assert settings.get("TASK_API_KEY") is None
    with pytest.raises(AttributeError):
        _ = settings.TASK_API_KEY
    assert settings.getenv("TASK_API_KEY") == SENSITIVE_TEST_VALUE
    assert SENSITIVE_TEST_VALUE not in json.dumps(settings.summary())


def test_summary_redacts_all_declared_business_values_and_existing_sensitive_builtin_values():
    settings = Settings({"BUSINESS_INT": 2, "BUSINESS_LABELS": [SENSITIVE_TEST_VALUE],
                         "BUSINESS_OPTIONS": {"token": SENSITIVE_TEST_VALUE},
                         "TASK_API_KEY": SENSITIVE_TEST_VALUE}, environ={
                            "REDIS_URL": "redis://synthetic-user:synthetic-password@localhost:6379/1",
                            "MONGO_URI": "mongodb://synthetic-user:synthetic-password@localhost:27017/test",
                            "DEFAULT_PROXY": "http://synthetic-user:synthetic-password@localhost:8080",
                        })
    summary = settings.summary()
    for key in ("BUSINESS_INT", "BUSINESS_LABELS", "BUSINESS_OPTIONS", "TASK_API_KEY",
                "REDIS_URL", "MONGO_URI", "DEFAULT_PROXY"):
        assert summary[key]["value"] == "[redacted]"
    assert summary["BUSINESS_INT"]["source"] == "task"
    assert summary["TASK_API_KEY"]["source"] == "task"
    assert summary["SEED_CONCURRENCY"]["value"] == 8
    serialized = json.dumps(summary)
    assert SENSITIVE_TEST_VALUE not in serialized and "synthetic-password" not in serialized


@pytest.mark.parametrize("cast,raw,expected", [
    (str, "001", "001"),
    (int, "42", 42),
    (float, "1.25", 1.25),
    (bool, "true", True),
    (bool, "FALSE", False),
    (bool, "1", True),
    (bool, "0", False),
    (list, '["one",2]', ["one", 2]),
    (dict, '{"one":2}', {"one": 2}),
    (tuple, '["one",2]', ("one", 2)),
])
def test_explicit_getenv_supports_safe_builtin_casts(cast, raw, expected):
    settings = Settings(environ={"UNDECLARED_OPTION": raw})
    value = settings.getenv("UNDECLARED_OPTION", cast=cast)
    assert value == expected and type(value) is type(expected)
    assert "UNDECLARED_OPTION" not in settings


def test_getenv_preserves_empty_values_and_uses_defaults_only_when_absent():
    settings = Settings(environ={"EMPTY": "", "NULL_TEXT": "null"})
    assert settings.getenv("EMPTY", "fallback") == ""
    assert settings.getenv("EMPTY", "fallback", cast=str) == ""
    assert settings.getenv("NULL_TEXT") is None
    assert settings.getenv("NULL_TEXT", cast=str) == "null"
    assert settings.getenv("MISSING") is None
    assert settings.getenv("MISSING", "fallback") == "fallback"
    assert settings.getenv("MISSING", 12) == 12
    with pytest.raises(ConfigurationError):
        settings.getenv("EMPTY", 12, cast=int)


@pytest.mark.parametrize("cast,bad_value", [
    (int, SENSITIVE_TEST_VALUE),
    (int, "2.5"),
    (float, SENSITIVE_TEST_VALUE),
    (float, "nan"),
    (float, "inf"),
    (bool, SENSITIVE_TEST_VALUE),
    (list, SENSITIVE_TEST_VALUE),
    (list, '{"wrong":"container"}'),
    (dict, '["wrong","container"]'),
    (tuple, '{"wrong":"container"}'),
])
def test_getenv_invalid_cast_is_configuration_error_without_value_in_exception_chain(cast, bad_value):
    settings = Settings(environ={"UNDECLARED_OPTION": bad_value})
    with pytest.raises(ConfigurationError) as captured:
        settings.getenv("UNDECLARED_OPTION", cast=cast)
    error = captured.value
    formatted = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert SENSITIVE_TEST_VALUE not in str(error)
    assert SENSITIVE_TEST_VALUE not in formatted
    assert SENSITIVE_TEST_VALUE not in json.dumps(settings.summary())


@pytest.mark.parametrize("default", [1, 1.5, True, [], {}, ()])
def test_invalid_declared_environment_does_not_expose_sensitive_value(default):
    bad_value = SENSITIVE_TEST_VALUE
    with pytest.raises(ConfigurationError) as captured:
        Settings({"BUSINESS_OPTION": default}, environ={"BUSINESS_OPTION": bad_value})
    error = captured.value
    formatted = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    assert bad_value not in str(error) and bad_value not in formatted


def test_getenv_never_evaluates_python_expressions(tmp_path):
    marker = tmp_path / "must-not-be-created"
    expression = f"__import__('pathlib').Path({str(marker)!r}).write_text('executed')"
    settings = Settings(environ={"PYTHON_EXPRESSION": expression})
    assert settings.getenv("PYTHON_EXPRESSION") == expression
    with pytest.raises(ConfigurationError):
        settings.getenv("PYTHON_EXPRESSION", cast=list)
    assert not marker.exists()


def test_two_crawlers_hold_independent_environment_and_business_snapshots(monkeypatch):
    monkeypatch.setattr("spider_man.config.os.environ", {})
    monkeypatch.setenv("SEED_CONCURRENCY", "3")
    monkeypatch.setenv("BUSINESS_LABELS", '["first"]')
    monkeypatch.setenv("TASK_API_KEY", "first-synthetic-api-key")
    first = Crawler("environment_test", "first", custom_settings={"BUSINESS_LABELS": ["default"]})
    monkeypatch.setenv("SEED_CONCURRENCY", "5")
    monkeypatch.setenv("BUSINESS_LABELS", '["second"]')
    monkeypatch.setenv("TASK_API_KEY", "second-synthetic-api-key")
    second = Crawler("environment_test", "second", custom_settings={"BUSINESS_LABELS": ["default"]})
    try:
        assert first.settings.SEED_CONCURRENCY == 3 and second.settings.SEED_CONCURRENCY == 5
        assert first.settings.BUSINESS_LABELS == ("first",) and second.settings.BUSINESS_LABELS == ("second",)
        assert first.settings.getenv("TASK_API_KEY") == "first-synthetic-api-key"
        assert second.settings.getenv("TASK_API_KEY") == "second-synthetic-api-key"
        first.settings = first.settings.with_cli({"SEED_CONCURRENCY": 2, "REQUEST_CONCURRENCY": 1})
        assert first.settings.SEED_CONCURRENCY == 3 and second.settings.SEED_CONCURRENCY == 5
        assert first.settings.REQUEST_CONCURRENCY == 1 and second.settings.REQUEST_CONCURRENCY == 16
        assert "TASK_API_KEY" not in first.settings.summary() and "TASK_API_KEY" not in second.settings.summary()
    finally:
        first.close()
        second.close()


def test_collection_environment_snapshots_are_independent_of_queue_identity(monkeypatch):
    monkeypatch.setenv("LOG_SOURCE", "first_source")
    monkeypatch.setenv("LOG_TOPIC", "first_topic")
    first = Crawler("queue_project", "first_task", custom_settings={"LOG_SOURCE": "task_source", "LOG_TOPIC": "task_topic"})
    monkeypatch.setenv("LOG_SOURCE", "second_source")
    monkeypatch.setenv("LOG_TOPIC", "second_topic")
    second = Crawler("queue_project", "second_task", custom_settings={"LOG_SOURCE": "task_source", "LOG_TOPIC": "task_topic"})
    try:
        assert first.collection_name == "first_source_first_topic"
        assert second.collection_name == "second_source_second_topic"
        assert first.settings.LOG_SOURCE == "first_source" and first.settings.LOG_TOPIC == "first_topic"
        assert second.settings.LOG_SOURCE == "second_source" and second.settings.LOG_TOPIC == "second_topic"
        assert first.settings.sources["LOG_SOURCE"] == second.settings.sources["LOG_SOURCE"] == "environment"
        assert first.todo_key == "{queue_project:first_task}:todo"
        assert second.todo_key == "{queue_project:second_task}:todo"
    finally:
        first.close()
        second.close()


def _setting_in_subprocess(environment):
    """全新解释器直接导入 setting，避免父进程的模块缓存掩盖读取行为。"""
    from spider_man import setting

    inherited = {key: value for key, value in os.environ.items()
                 if key not in setting._defaults and key != "SPIDER_THREAD_NUM"
                 and not key.startswith("SPIDER_MAN_")}
    inherited.update(environment)
    result = subprocess.run([
        sys.executable, "-c",
        "import json\n"
        "import spider_man.setting as setting\n"
        "from spider_man.config import Settings\n"
        "snapshot = Settings(environ={})\n"
        "values = {name:getattr(setting,name) for name in setting._defaults}\n"
        "print(json.dumps({'values':values,'defaults':setting._defaults,"
        "'instance_defaults':{name:snapshot[name] for name in setting._defaults},"
        "'types':{name:type(value).__name__ for name,value in values.items()}}))\n",
    ], cwd=Path(__file__).resolve().parents[1], env=inherited,
        capture_output=True, text=True, timeout=20, check=True)
    return json.loads(result.stdout)


def test_direct_setting_import_retains_all_pure_defaults_without_environment():
    from spider_man import setting

    result = _setting_in_subprocess({})
    expected = json.loads(json.dumps(setting._defaults))
    assert result["defaults"] == result["values"] == result["instance_defaults"] == expected
    assert result["types"] == {name: type(value).__name__ for name, value in setting._defaults.items()}


def test_direct_setting_import_reads_unprefixed_scalar_container_and_nullable_values():
    environment = {
        "SEED_CONCURRENCY": "3", "REQUEST_INTERVAL": "0.125", "LOG_JSON": "false",
        "LOG_LEVEL": "DEBUG", "MONGO_DATABASE": "environment_database",
        "LOG_SOURCE": "environment_source", "LOG_TOPIC": "environment_topic",
        "MONGO_URI": "null", "MONGO_UPSERT_KEYS": '["aweme_id"]',
        "ITEM_PIPELINES": '["spider_man.pipelines.mongo_pipeline.MongoPipeline"]',
        "DEFAULT_PROXY": '{"https":"http://proxy.invalid:8080"}',
    }
    result = _setting_in_subprocess(environment)
    expected = {
        "SEED_CONCURRENCY": 3, "REQUEST_INTERVAL": 0.125, "LOG_JSON": False,
        "LOG_LEVEL": "DEBUG", "MONGO_DATABASE": "environment_database",
        "LOG_SOURCE": "environment_source", "LOG_TOPIC": "environment_topic",
        "MONGO_URI": None, "MONGO_UPSERT_KEYS": ["aweme_id"],
        "ITEM_PIPELINES": ["spider_man.pipelines.mongo_pipeline.MongoPipeline"],
        "DEFAULT_PROXY": {"https": "http://proxy.invalid:8080"},
    }
    for name, value in expected.items():
        assert result["values"][name] == value
        assert result["types"][name] == type(value).__name__
    # 读取环境不会改写纯默认源，后续实例可独立建立自己的快照。
    from spider_man import setting
    assert result["defaults"] == json.loads(json.dumps(setting._defaults))
    assert result["instance_defaults"] == result["defaults"]


def test_direct_setting_import_does_not_expand_prefixed_environment():
    result = _setting_in_subprocess({
        "SPIDER_MAN_SEED_CONCURRENCY": "99", "SPIDER_MAN_REQUEST_INTERVAL": "invalid-but-ignored",
        "SPIDER_MAN_LOG_JSON": "invalid-but-ignored", "SPIDER_MAN_LOG_SOURCE": "ignored-source",
        "SPIDER_MAN_LOG_TOPIC": "ignored-topic",
    })
    assert result["values"] == result["defaults"]


def test_every_public_framework_setting_explicitly_reads_its_same_named_environment():
    from spider_man import setting

    source = ast.parse(Path(setting.__file__).read_text(encoding="utf-8"))
    assignments = {}
    for node in source.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, ast.AnnAssign) else []
        for target in targets:
            if isinstance(target, ast.Name) and target.id.isupper() and not target.id.startswith("_"):
                assignments[target.id] = node.value
    public = {name for name in vars(setting) if name.isupper() and not name.startswith("_")}
    assert "MONGO_COLLECTION" not in public
    assert len(public) == 40
    assert public == set(setting._defaults) == set(assignments)
    for name, value in assignments.items():
        calls = [node for node in ast.walk(value)
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                 and isinstance(node.func.value, ast.Name) and node.func.value.id == "os"
                 and node.func.attr == "getenv"]
        assert any(call.args and isinstance(call.args[0], ast.Constant) and call.args[0].value == name
                   and (len(call.args) >= 2 or any(keyword.arg == "default" for keyword in call.keywords))
                   for call in calls), f"{name} must explicitly read os.getenv({name!r}, default)"

from collections import Counter
from types import SimpleNamespace
import threading
import time

import mongomock
import pytest
from pymongo.errors import AutoReconnect, DuplicateKeyError
from pymongo.write_concern import WriteConcern

from spider_man.base import Item, Seed
from spider_man.db.mongodb import MongoDB
from spider_man.exceptions import ConfigurationError, DropItem, ItemValidationError, LeaseLost, StorageError
from spider_man.pipelines import (
    ConsolePipeline, MongoPipeline, PipelineBatchError, PipelineManager, PipelineResult,
)


class Metrics:
    def __init__(self):
        self.values = Counter()

    def counter(self, name, count):
        self.values[name] += count


@pytest.fixture
def app():
    return SimpleNamespace(
        mongo=MongoDB(client=mongomock.MongoClient(), db="test"),
        collection_name="bilibili_user_post", metrics=Metrics(),
        settings={"MONGO_UPSERT_KEYS": None, "ITEM_BATCH_SIZE": 4,
                  "ITEM_FLUSH_INTERVAL": 0.02, "ITEM_BUFFER_LIMIT": 8,
                  "ITEM_MAX_RETRIES": 2, "ITEM_RETRY_TIMEOUT": 1,
                  "ITEM_RETRY_BACKOFF": 0.001, "ITEM_RETRY_BACKOFF_MAX": 0.005},
    )


def item(data):
    return Item(data=data, seed=Seed({"user_id": 1}))


def test_mongo_adapter_indexes_and_direct_access(app):
    assert app.mongo.ping()["ok"] == 1
    name = app.mongo.create_index(app.collection_name, ["bvid", ("part", -1)])
    assert app.mongo.get_index(app.collection_name)[name]["key"] == [("bvid", 1), ("part", -1)]
    assert app.mongo[app.collection_name].name == app.collection_name
    assert app.mongo.db.name == "test"
    with pytest.raises(ConfigurationError):
        app.mongo.create_index(app.collection_name, "bvid")


def test_insert_mode_does_not_deduplicate(app):
    pipeline = MongoPipeline(app)
    pipeline.open()
    assert pipeline.upsert_keys is None
    first = item({"name": "same", "created_at": "business time"})
    assert pipeline.process_item(first).status == "inserted"
    assert pipeline.process_item(first).status == "inserted"
    documents = list(app.mongo[app.collection_name].find())
    assert len(documents) == 2
    assert documents[0]["created_at"] == "business time"
    assert "created_at" in documents[0]["_spider_man"]
    assert "_id" not in first.data


def test_unique_key_merge_and_confirmation_statuses(app):
    app.mongo.create_index(app.collection_name, ["bvid"])
    pipeline = MongoPipeline(app)
    pipeline.open()
    assert pipeline.upsert_keys == ("bvid",)
    assert pipeline.process_item(item({"bvid": "a", "name": "old", "keep": 7})).status == "inserted"
    before = app.mongo[app.collection_name].find_one({"bvid": "a"})
    assert pipeline.process_item(item({"bvid": "a", "name": None})).status == "updated"
    assert pipeline.process_item(item({"bvid": "a", "name": None})).status == "unchanged"
    stored = app.mongo[app.collection_name].find_one({"bvid": "a"})
    assert stored["keep"] == 7
    assert stored["name"] is None
    assert stored["_spider_man"]["created_at"] == before["_spider_man"]["created_at"]
    assert app.mongo[app.collection_name].count_documents({}) == 1


def test_compound_and_nested_unique_keys(app):
    app.mongo.create_index(app.collection_name, ["user.id", "part"])
    pipeline = MongoPipeline(app)
    pipeline.open()
    assert pipeline.process_item(item({"user": {"id": 1}, "part": 2})).status == "inserted"
    assert pipeline.process_item(item({"user": {"id": 1}, "part": 2, "new": True})).status == "updated"


@pytest.mark.parametrize("data", [{}, {"bvid": None}, {"bvid": []}, {"bvid": {}}, {"bvid": float("nan")}])
def test_invalid_unique_keys_never_fall_back_to_insert(app, data):
    app.mongo.create_index(app.collection_name, ["bvid"])
    pipeline = MongoPipeline(app)
    pipeline.open()
    with pytest.raises(ItemValidationError):
        pipeline.process_item(item(data))
    assert app.mongo[app.collection_name].count_documents({}) == 0


@pytest.mark.parametrize("data", [{"_spider_man": {}}, {"a.b": 1}, {"$evil": 1}, {"_id": []}, {"bad": object()}])
def test_reserved_metadata_and_invalid_bson_rejected(app, data):
    pipeline = MongoPipeline(app)
    pipeline.open()
    with pytest.raises(ItemValidationError):
        pipeline.process_item(item(data))


@pytest.mark.parametrize("options", [{"sparse": True}, {"partialFilterExpression": {"bvid": {"$exists": True}}}, {"collation": {"locale": "en"}}])
def test_unsupported_unique_indexes_fail_at_start(app, options, monkeypatch):
    coll = app.mongo[app.collection_name]
    # mongomock does not preserve collation, so simulate index metadata itself.
    monkeypatch.setattr(coll, "index_information", lambda: {
        "_id_": {"key": [("_id", 1)]},
        "business": {"key": [("bvid", 1)], "unique": True, **options},
    })
    with pytest.raises(ConfigurationError, match="Unsupported"):
        MongoPipeline(app).open()


def test_multiple_indexes_require_explicit_matching_keys(app):
    app.mongo.create_index(app.collection_name, ["bvid"])
    app.mongo.create_index(app.collection_name, ["slug"])
    with pytest.raises(ConfigurationError, match="Multiple"):
        MongoPipeline(app).open()
    app.settings["MONGO_UPSERT_KEYS"] = ["bvid"]
    pipeline = MongoPipeline(app)
    pipeline.open()
    assert pipeline.upsert_keys == ("bvid",)
    app.settings["MONGO_UPSERT_KEYS"] = ["missing"]
    with pytest.raises(ConfigurationError, match="existing"):
        MongoPipeline(app).open()


def test_explicit_id_identity_and_immutable_id(app):
    app.settings["MONGO_UPSERT_KEYS"] = ["_id"]
    pipeline = MongoPipeline(app)
    pipeline.open()
    assert pipeline.process_item(item({"_id": "business-id", "value": 1})).status == "inserted"
    assert pipeline.process_item(item({"_id": "business-id", "value": 2})).status == "updated"
    app.mongo.create_index(app.collection_name, ["bvid"])
    app.settings["MONGO_UPSERT_KEYS"] = ["bvid"]
    pipeline.open()
    pipeline.process_item(item({"_id": "one", "bvid": "a", "name": "original"}))
    with pytest.raises(ItemValidationError):
        pipeline.process_item(item({"_id": "two", "bvid": "a", "name": "overwritten"}))
    assert app.mongo[app.collection_name].find_one({"bvid": "a"})["name"] == "original"


def test_conflict_with_other_unique_index_is_not_success(app):
    app.mongo.create_index(app.collection_name, ["bvid"])
    app.mongo.create_index(app.collection_name, ["slug"])
    app.settings["MONGO_UPSERT_KEYS"] = ["bvid"]
    pipeline = MongoPipeline(app)
    pipeline.open()
    pipeline.process_item(item({"bvid": "a", "slug": "reserved"}))
    with pytest.raises(StorageError):
        pipeline.process_item(item({"bvid": "b", "slug": "reserved"}))
    assert app.mongo[app.collection_name].find_one({"bvid": "b"}) is None
    pipeline.process_item(item({"bvid": "b", "slug": "free"}))
    with pytest.raises(StorageError):
        pipeline.process_item(item({"bvid": "b", "slug": "reserved"}))
    assert app.mongo[app.collection_name].find_one({"bvid": "b"})["slug"] == "free"


def test_racing_upsert_retries_only_verified_selected_key(app, monkeypatch):
    app.mongo.create_index(app.collection_name, ["bvid"])
    pipeline = MongoPipeline(app)
    pipeline.open()
    original = pipeline.collection.find_one_and_update
    calls = []

    def race(query, update, **kwargs):
        calls.append(kwargs["upsert"])
        if len(calls) == 1:
            pipeline.collection.insert_one({"bvid": "a", "value": 0})
            raise DuplicateKeyError("racing insertion", details={"keyPattern": {"bvid": 1}})
        return original(query, update, **kwargs)

    monkeypatch.setattr(pipeline.collection, "find_one_and_update", race)
    assert pipeline.process_item(item({"bvid": "a", "value": 1})).status == "updated"
    assert calls == [True, False]
    assert pipeline.collection.count_documents({}) == 1


def test_unacknowledged_storage_rejected(app):
    collection = app.mongo[app.collection_name].with_options(write_concern=WriteConcern(w=0))
    app.mongo.get_collection = lambda _: collection
    with pytest.raises(ConfigurationError, match="acknowledged"):
        MongoPipeline(app).open()


class RecordingStore:
    required = True
    persistent = True

    def __init__(self):
        self.calls = []

    def process_item(self, item):
        self.calls.append(dict(item.data))
        return PipelineResult("inserted")


def test_manager_requires_persistence_and_explicit_debug_override(app):
    manager = PipelineManager(app, pipelines=[ConsolePipeline(app)])
    with pytest.raises(ConfigurationError, match="persistent"):
        manager.open()
    app.settings["ALLOW_CONSOLE_ONLY"] = True
    manager = PipelineManager(app, pipelines=[ConsolePipeline(app)])
    manager.open()
    assert manager.submit(item({"a": 1})).result(1).status == "unchanged"
    manager.close(1)


def test_transform_drop_and_multiple_persistence_targets(app):
    class Transform:
        def process_item(self, value):
            if value.data.get("drop"):
                raise DropItem("business filter")
            return {**value.data, "clean": True}

    first, second = RecordingStore(), RecordingStore()
    manager = PipelineManager(app, pipelines=[Transform(), first, second])
    manager.open()
    saved = manager.submit(item({"a": 1}))
    dropped = manager.submit(item({"drop": True}))
    assert saved.result(1).status == "inserted"
    assert dropped.result(1) == PipelineResult("dropped", "business filter")
    manager.close(1)
    assert first.calls == second.calls == [{"a": 1, "clean": True}]
    assert app.metrics.values == {"item_inserted": 1, "item_dropped": 1}


def test_batch_partial_success_retries_only_unconfirmed_items(app):
    class PartialStore(RecordingStore):
        def process_batch(self, items):
            self.calls.append([value.data["id"] for value in items])
            if len(self.calls) == 1:
                raise PipelineBatchError([PipelineResult("inserted"), AutoReconnect("temporary")])
            return [PipelineResult("updated") for _ in items]

    store = PartialStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    first = manager.submit(item({"id": 1}))
    second = manager.submit(item({"id": 2}))
    assert first.result(1).status == "inserted"
    assert second.result(1).status == "updated"
    assert store.calls == [[1, 2], [2]]
    assert manager.healthy.is_set()
    manager.close(1)
    assert app.metrics.values == {"item_inserted": 1, "item_updated": 1}


def test_successful_earlier_storage_not_retried_when_later_stage_fails(app):
    class FlakyStore(RecordingStore):
        def process_item(self, item):
            self.calls.append(dict(item.data))
            if len(self.calls) == 1:
                raise AutoReconnect("temporary")
            return PipelineResult("updated")

    first, second = RecordingStore(), FlakyStore()
    manager = PipelineManager(app, pipelines=[first, second])
    manager.open()
    assert manager.submit(item({"id": 1})).result(1).status == "updated"
    manager.close(1)
    assert len(first.calls) == 1
    assert len(second.calls) == 2


def test_retry_budget_exhaustion_failure_and_health_recovery(app):
    class DownStore(RecordingStore):
        recovered = False

        def process_item(self, item):
            self.calls.append(dict(item.data))
            raise AutoReconnect("server down")

        def check_health(self):
            if not self.recovered:
                raise AutoReconnect("server down")

    store = DownStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    future = manager.submit(item({"id": 1}))
    with pytest.raises(StorageError, match="budget exhausted"):
        future.result(1)
    assert len(store.calls) == 3  # first attempt plus two configured retries
    assert not manager.healthy.is_set()
    store.recovered = True
    assert manager.healthy.wait(1)
    manager.close(1)
    assert app.metrics.values["item_failed"] == 1


def test_permanent_item_failure_does_not_retry(app):
    class InvalidStore(RecordingStore):
        def process_item(self, item):
            self.calls.append(item)
            raise ItemValidationError("missing key")

    store = InvalidStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    with pytest.raises(ItemValidationError):
        manager.submit(item({"id": 1})).result(1)
    manager.close(1)
    assert len(store.calls) == 1


def test_total_retry_timeout_caps_wait(app):
    class DownStore(RecordingStore):
        def process_item(self, item):
            self.calls.append(item)
            raise AutoReconnect("down")

    app.settings.update(ITEM_RETRY_TIMEOUT=0.02, ITEM_RETRY_BACKOFF=0.1, ITEM_RETRY_BACKOFF_MAX=0.1, ITEM_MAX_RETRIES=20)
    store = DownStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    with pytest.raises(StorageError, match="timeout"):
        manager.submit(item({"id": 1})).result(1)
    manager.close(1)
    assert len(store.calls) == 1


def test_buffer_bound_includes_inflight_batch_and_shutdown_does_not_confirm(app):
    started, release = threading.Event(), threading.Event()

    class BlockingStore(RecordingStore):
        def process_item(self, item):
            started.set()
            release.wait(2)
            return PipelineResult("inserted")

    app.settings.update(ITEM_BUFFER_LIMIT=1, ITEM_BATCH_SIZE=1)
    manager = PipelineManager(app, pipelines=[BlockingStore()])
    manager.open()
    future = manager.submit(item({"id": 1}))
    assert started.wait(1)
    assert manager.size == 1
    assert not future.done()
    with pytest.raises(TimeoutError, match="full"):
        manager.submit(item({"id": 2}), timeout=0.01)
    with pytest.raises(TimeoutError, match="unconfirmed"):
        manager.close(0.01)
    assert not future.done()
    release.set()
    assert future.result(1).status == "inserted"
    manager.close(1)


def test_batch_size_interval_flush_and_per_item_outcome(app):
    class BatchStore(RecordingStore):
        def process_batch(self, items):
            self.calls.append([value.data["id"] for value in items])
            return [PipelineResult("inserted") for _ in items]

    app.settings.update(ITEM_BATCH_SIZE=3, ITEM_FLUSH_INTERVAL=0.1)
    store = BatchStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    futures = [manager.submit(item({"id": value})) for value in range(4)]
    manager.flush(1)
    assert [future.result().status for future in futures] == ["inserted"] * 4
    assert [len(batch) for batch in store.calls] == [3, 1]
    assert manager.size == 0
    manager.close(1)


def test_batch_protocol_and_persistent_confirmation_errors_fail_items(app):
    class BrokenBatch(RecordingStore):
        def process_batch(self, items):
            return True

    for store in (BrokenBatch(), SimpleNamespace(required=True, persistent=True, process_item=lambda _: None)):
        manager = PipelineManager(app, pipelines=[store])
        manager.open()
        with pytest.raises(ConfigurationError):
            manager.submit(item({"id": 1})).result(1)
        manager.close(1)


def test_metrics_failure_does_not_change_storage_success(app):
    app.metrics.counter = lambda *args: (_ for _ in ()).throw(RuntimeError("metrics down"))
    manager = PipelineManager(app, pipelines=[RecordingStore()])
    manager.open()
    assert manager.submit(item({"id": 1})).result(1).status == "inserted"
    manager.close(1)


def test_lost_seed_lease_skips_unstarted_items(app):
    store = RecordingStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    with pytest.raises(LeaseLost):
        manager.submit(item({"id": 1}), valid=lambda: False).result(1)
    manager.close(1)
    assert store.calls == []


def test_lease_guard_connection_failure_does_not_confirm_or_write(app):
    from redis.exceptions import ConnectionError

    store = RecordingStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()

    def unavailable():
        raise ConnectionError("ownership cannot be checked")

    with pytest.raises(ConnectionError):
        manager.submit(item({"id": 1}), valid=unavailable).result(1)
    manager.close(1)
    assert store.calls == []
    assert app.metrics.values["item_inserted"] == 0


def test_submission_payload_is_frozen_before_background_write(app):
    store = RecordingStore()
    manager = PipelineManager(app, pipelines=[store])
    manager.open()
    original = item({"nested": {"value": "original"}})
    future = manager.submit(original)
    original.data["nested"]["value"] = "changed"
    assert future.result(1).status == "inserted"
    manager.close(1)
    assert store.calls == [{"nested": {"value": "original"}}]


def test_retry_timeout_budget_is_shared_across_storage_stages(app):
    class SlowStore(RecordingStore):
        def process_item(self, item):
            time.sleep(0.03)
            return super().process_item(item)

    class DownStore(RecordingStore):
        def process_item(self, item):
            self.calls.append(item)
            raise AutoReconnect("down")

    app.settings["ITEM_RETRY_TIMEOUT"] = 0.02
    first, second = SlowStore(), DownStore()
    manager = PipelineManager(app, pipelines=[first, second])
    manager.open()
    with pytest.raises(StorageError, match="timeout"):
        manager.submit(item({"id": 1})).result(1)
    manager.close(1)
    assert len(first.calls) == len(second.calls) == 1


def test_mongo_outage_stops_repeated_probes_within_batch(app, monkeypatch):
    pipeline = MongoPipeline(app)
    pipeline.open()
    calls = []

    def down(value):
        calls.append(value.data["id"])
        if value.data["id"] == 1:
            return PipelineResult("inserted")
        raise StorageError("Mongo unavailable", retryable=True)

    monkeypatch.setattr(pipeline, "process_item", down)
    results = pipeline.process_batch([item({"id": index}) for index in range(1, 5)])
    assert results[0].status == "inserted"
    assert all(isinstance(error, StorageError) for error in results[1:])
    assert calls == [1, 2]

"""Exercise MongoDB server behavior that an in-memory mock cannot establish."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
import threading

import pytest
from pymongo.errors import DuplicateKeyError, OperationFailure

from spider_man import Crawler
from spider_man.base import Item, Seed
from spider_man.exceptions import ConfigurationError, ItemValidationError, StorageError
from spider_man.pipelines import MongoPipeline


@pytest.fixture
def real_app(real_mongo):
    return SimpleNamespace(mongo=real_mongo, collection_name="bilibili_user_post",
                           settings={"MONGO_UPSERT_KEYS": None})


def item(data):
    return Item(data=data, seed=Seed({"page": 1}))


def test_real_compound_upsert_merge_metadata_and_immutable_id(real_app):
    app = real_app
    app.mongo.create_index(app.collection_name, ["user_id", ("bvid", -1)])
    pipeline = MongoPipeline(app)
    pipeline.open()
    assert pipeline.process_item(item({"_id": "chosen", "user_id": 1, "bvid": "a", "name": "first", "keep": 7})).status == "inserted"
    original = pipeline.collection.find_one({"_id": "chosen"})
    assert pipeline.process_item(item({"user_id": 1, "bvid": "a", "name": None})).status == "updated"
    assert pipeline.process_item(item({"user_id": 1, "bvid": "a", "name": None})).status == "unchanged"
    current = pipeline.collection.find_one({"_id": "chosen"})
    assert current["keep"] == 7 and current["name"] is None
    assert original["_spider_man"]["created_at"] == current["_spider_man"]["created_at"]
    with pytest.raises(ItemValidationError, match="_id"):
        pipeline.process_item(item({"_id": "replacement", "user_id": 1, "bvid": "a", "name": "wrong"}))
    assert pipeline.collection.find_one({"_id": "chosen"})["name"] is None


def test_real_id_default_is_not_business_upsert_key(real_app):
    pipeline = MongoPipeline(real_app)
    pipeline.open()
    pipeline.process_item(item({"value": "repeat"}))
    pipeline.process_item(item({"value": "repeat"}))
    assert pipeline.collection.count_documents({"value": "repeat"}) == 2
    real_app.settings["MONGO_UPSERT_KEYS"] = ["_id"]
    pipeline.open()
    assert pipeline.process_item(item({"_id": "same", "value": 1})).status == "inserted"
    assert pipeline.process_item(item({"_id": "same", "value": 2})).status == "updated"
    assert pipeline.collection.count_documents({"_id": "same"}) == 1


def test_real_concurrent_upserts_keep_one_business_record(real_app):
    real_app.mongo.create_index(real_app.collection_name, ["bvid"])
    pipeline = MongoPipeline(real_app)
    pipeline.open()
    barrier = threading.Barrier(16)

    def write(index):
        barrier.wait(timeout=5)
        return pipeline.process_item(item({"bvid": "concurrent", "attempt": index})).status

    with ThreadPoolExecutor(max_workers=16) as executor:
        statuses = list(executor.map(write, range(16)))
    assert Counter(statuses)["inserted"] == 1
    assert len(statuses) == 16
    assert pipeline.collection.count_documents({"bvid": "concurrent"}) == 1


def test_real_other_unique_index_conflicts_fail(real_app):
    app = real_app
    app.mongo.create_index(app.collection_name, ["bvid"])
    app.mongo.create_index(app.collection_name, ["slug"])
    with pytest.raises(ConfigurationError, match="Multiple"):
        MongoPipeline(app).open()
    app.settings["MONGO_UPSERT_KEYS"] = ["bvid"]
    pipeline = MongoPipeline(app)
    pipeline.open()
    pipeline.process_item(item({"bvid": "a", "slug": "taken"}))
    pipeline.process_item(item({"bvid": "b", "slug": "free"}))
    for data in ({"bvid": "c", "slug": "taken"}, {"bvid": "b", "slug": "taken"}):
        with pytest.raises(StorageError):
            pipeline.process_item(item(data))
    assert pipeline.collection.find_one({"bvid": "b"})["slug"] == "free"
    assert pipeline.collection.find_one({"bvid": "c"}) is None


@pytest.mark.parametrize("options", [
    {"sparse": True},
    {"partialFilterExpression": {"bvid": {"$exists": True}}},
    {"collation": {"locale": "en", "strength": 2}},
])
def test_real_unsupported_unique_indexes_fail_startup(real_app, options):
    real_app.mongo.create_index(real_app.collection_name, ["bvid"], **options)
    with pytest.raises(ConfigurationError, match="Unsupported"):
        MongoPipeline(real_app).open()


def test_real_index_creation_conflicts_are_not_swallowed(real_app):
    app = real_app
    app.mongo[app.collection_name].insert_many([{"bvid": "duplicate"}, {"bvid": "duplicate"}])
    with pytest.raises(DuplicateKeyError):
        app.mongo.create_index(app.collection_name, ["bvid"])
    app.mongo[app.collection_name].delete_many({})
    app.mongo.create_index(app.collection_name, ["bvid"], unique=False)
    with pytest.raises(OperationFailure):
        app.mongo.create_index(app.collection_name, ["bvid"], unique=True)


def test_real_validation_and_partial_batch_confirmation(real_app):
    real_app.mongo.create_index(real_app.collection_name, ["bvid"])
    pipeline = MongoPipeline(real_app)
    pipeline.open()
    results = pipeline.process_batch([item({"bvid": "good"}), item({"bvid": None}), item({"bvid": "other"})])
    assert results[0].status == "inserted" and results[2].status == "inserted"
    assert isinstance(results[1], ItemValidationError)
    assert pipeline.collection.count_documents({}) == 2
    with pytest.raises(ItemValidationError, match="reserved"):
        pipeline.process_item(item({"bvid": "bad", "_spider_man": {"created_at": "user"}}))


def test_real_crawler_log_source_topic_routes_mongo_without_changing_redis_identity(real_mongo, monkeypatch):
    monkeypatch.setenv("LOG_SOURCE", "archive")
    monkeypatch.setenv("LOG_TOPIC", "creator_posts")
    monkeypatch.delenv("MONGO_UPSERT_KEYS", raising=False)
    crawler = Crawler(
        project="bilibili",
        task="user_post",
        mongo=real_mongo,
        custom_settings={
            "LOG_SOURCE": "task_source",
            "LOG_TOPIC": "task_topic",
            "MONGO_UPSERT_KEYS": ["bvid"],
        },
    )
    assert crawler.collection_name == "archive_creator_posts"
    assert crawler.settings.sources["LOG_SOURCE"] == "environment"
    assert crawler.settings.sources["LOG_TOPIC"] == "environment"
    assert (crawler.project, crawler.task) == ("bilibili", "user_post")
    assert crawler.todo_key == "{bilibili:user_post}:todo"
    assert crawler.queue is None

    real_mongo.create_index(crawler.collection_name, ["bvid"], unique=True)
    pipeline = MongoPipeline(crawler)
    pipeline.open()
    try:
        assert pipeline.process_item(item({"bvid": "same", "title": "original", "keep": True})).status == "inserted"
        assert pipeline.process_item(item({"bvid": "same", "title": "updated"})).status == "updated"
        assert pipeline.process_item(item({"bvid": "same", "title": "updated"})).status == "unchanged"
        pipeline.flush()
    finally:
        pipeline.close()

    stored = real_mongo["archive_creator_posts"]
    assert stored.count_documents({}) == 1
    document = stored.find_one({"bvid": "same"})
    assert document["title"] == "updated" and document["keep"] is True
    assert real_mongo["bilibili_user_post"].count_documents({}) == 0
    assert real_mongo.db.list_collection_names() == ["archive_creator_posts"]
    assert crawler.todo_key == "{bilibili:user_post}:todo"
    assert crawler.queue is None

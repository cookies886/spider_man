"""Acknowledged MongoDB item persistence with business-key upserts."""

from __future__ import annotations

import copy
import logging
import math
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone

from bson import BSON, ObjectId
from bson.decimal128 import Decimal128
from bson.errors import InvalidDocument
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

from spider_man.exceptions import ConfigurationError, ItemValidationError
from spider_man.pipelines._storage import _recoverable, _storage_error
from spider_man.pipelines.base_pipeline import BasePipeline, PipelineResult

log = logging.getLogger(__name__)
METADATA_FIELD = "_spider_man"


def _field_value(document, path):
    current = document
    for component in path.split("."):
        if not isinstance(current, Mapping) or component not in current:
            raise ItemValidationError(f"Item is missing unique key field {path!r}")
        current = current[component]
    if current is None or not isinstance(current, (str, int, float, bool, bytes, datetime, ObjectId, Decimal128)):
        raise ItemValidationError(f"Unique key {path!r} must be a non-null BSON scalar")
    if isinstance(current, float) and not math.isfinite(current):
        raise ItemValidationError(f"Unique key {path!r} must be finite")
    return current


def _validated_data(item):
    if not isinstance(getattr(item, "data", None), Mapping):
        raise ItemValidationError("Item.data must be a mapping")
    document = copy.deepcopy(dict(item.data))
    if METADATA_FIELD in document:
        raise ItemValidationError(f"{METADATA_FIELD!r} is reserved for framework metadata")
    for key in document:
        if not isinstance(key, str) or not key or key.startswith("$") or "." in key:
            raise ItemValidationError("Top-level Item fields must be nonempty strings without dots or a leading '$'")
    try:
        BSON.encode(document)
    except (InvalidDocument, OverflowError, TypeError, ValueError) as error:
        raise ItemValidationError("Item contains data that cannot be encoded as BSON") from error
    if "_id" in document:
        _field_value(document, "_id")
    return document


class MongoPipeline(BasePipeline):
    required = True
    persistent = True

    def __init__(self, app):
        self.app = app
        self.collection = None
        self.upsert_keys = None
        self.index_name = None

    def open(self):
        self.upsert_keys = None
        self.index_name = None
        self.collection = self.app.mongo.get_collection(self.app.collection_name)
        if not self.collection.write_concern.acknowledged:
            raise ConfigurationError("MongoPipeline requires acknowledged MongoDB writes (w must not be 0)")
        indexes = self.collection.index_information()
        candidates = []
        for name, index in indexes.items():
            keys = index.get("key", [])
            if keys == [("_id", 1)]:
                continue
            if not index.get("unique"):
                continue
            if (index.get("sparse") or index.get("partialFilterExpression") is not None
                    or index.get("collation") is not None
                    or any(direction not in (1, -1) for _, direction in keys)):
                raise ConfigurationError(f"Unsupported business unique index {name!r}: sparse, partial, collation, and special indexes need an adapter")
            if not keys or any(field == METADATA_FIELD or field.startswith(METADATA_FIELD + ".") for field, _ in keys):
                raise ConfigurationError(f"Unique index {name!r} uses reserved framework metadata")
            candidates.append((name, tuple(field for field, _ in keys)))
        explicit = self.app.settings.get("MONGO_UPSERT_KEYS")
        if explicit is not None:
            if isinstance(explicit, str) or not isinstance(explicit, Sequence) or not explicit or any(not isinstance(key, str) or not key for key in explicit):
                raise ConfigurationError("MONGO_UPSERT_KEYS must be a nonempty list of fields or None")
            explicit = tuple(explicit)
            if len(set(explicit)) != len(explicit):
                raise ConfigurationError("MONGO_UPSERT_KEYS cannot contain duplicate fields")
            if explicit == ("_id",):
                # MongoDB always enforces its default _id identity, including an empty collection.
                self.index_name, self.upsert_keys = "_id_", explicit
            else:
                matches = [(name, keys) for name, keys in candidates if keys == explicit]
                if not matches:
                    raise ConfigurationError("MONGO_UPSERT_KEYS must match an existing supported unique index in index order")
                self.index_name, self.upsert_keys = matches[0]
        elif len(candidates) > 1:
            raise ConfigurationError("Multiple business unique indexes exist; specify MONGO_UPSERT_KEYS")
        elif candidates:
            self.index_name, self.upsert_keys = candidates[0]
        log.info("Mongo write policy collection=%s mode=%s index=%s keys=%s", self.app.collection_name,
                 "upsert" if self.upsert_keys else "insert", self.index_name, self.upsert_keys)

    def process_item(self, item):
        if self.collection is None:
            raise RuntimeError("MongoPipeline is not open")
        document = _validated_data(item)
        now = datetime.now(timezone.utc)
        if not self.upsert_keys:
            document[METADATA_FIELD] = {"created_at": now, "updated_at": now}
            try:
                result = self.collection.insert_one(document)
                if not result.acknowledged:
                    raise _storage_error("MongoDB insert was not acknowledged")
                return PipelineResult("inserted")
            except PyMongoError as error:
                raise _storage_error("MongoDB insert failed", retryable=_recoverable(error)) from error

        selected = {key: _field_value(document, key) for key in self.upsert_keys}
        query = dict(selected)
        if "_id" in document:
            query["_id"] = document["_id"]
        business = {key: value for key, value in document.items() if key != "_id"}
        update = {"$set": {**business, f"{METADATA_FIELD}.updated_at": now},
                  "$setOnInsert": {f"{METADATA_FIELD}.created_at": now}}
        try:
            previous = self.collection.find_one_and_update(query, update, upsert=True, return_document=ReturnDocument.BEFORE)
        except DuplicateKeyError as error:
            # A racing upsert on the selected key is safe to turn into an update.
            # An unrelated index collision must remain a failure.
            pattern = (error.details or {}).get("keyPattern")
            if pattern is not None and tuple(pattern) != self.upsert_keys:
                raise _storage_error("MongoDB write conflicts with another unique index") from error
            existing = self.collection.find_one(selected)
            if existing is None:
                raise _storage_error("Duplicate key does not match the selected upsert identity") from error
            if "_id" in document and document["_id"] != existing.get("_id"):
                raise ItemValidationError("Item _id cannot replace the existing document identity") from error
            try:
                previous = self.collection.find_one_and_update(query, update, upsert=False, return_document=ReturnDocument.BEFORE)
                if previous is None:
                    raise _storage_error("Selected document disappeared during concurrent upsert", retryable=True)
            except PyMongoError as retry_error:
                raise _storage_error("MongoDB update conflicts or failed", retryable=_recoverable(retry_error)) from retry_error
        except PyMongoError as error:
            raise _storage_error("MongoDB upsert failed", retryable=_recoverable(error)) from error
        if previous is None:
            return PipelineResult("inserted")
        unchanged = all(key in previous and previous[key] == value for key, value in business.items())
        return PipelineResult("unchanged" if unchanged else "updated")

    def process_batch(self, items):
        results = []
        for index, item in enumerate(items):
            try:
                results.append(self.process_item(item))
            except Exception as error:
                results.append(error)
                if _recoverable(error):
                    # Stop probing the same outage once per remaining Item. They
                    # are unconfirmed and eligible for the same bounded retry.
                    results.extend([error] * (len(items) - index - 1))
                    break
        return results

    def check_health(self):
        self.app.mongo.ping()

    def flush(self):
        pass

    def close(self):
        # The Crawler owns and closes the database client.
        pass

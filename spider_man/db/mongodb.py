"""Small PyMongo adapter. Database errors are never converted to success values."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from pymongo import ASCENDING, MongoClient

from spider_man.exceptions import ConfigurationError


class MongoDB:
    def __init__(self, url=None, db=None, client=None, **kwargs):
        self.client = client if client is not None else MongoClient(url, **kwargs)
        try:
            self.db = self.client.get_database(db)
        except Exception:
            if client is None:
                self.client.close()
            raise

    @classmethod
    def from_url(cls, url, **kwargs):
        return cls(url=url, **kwargs)

    def ping(self):
        return self.client.admin.command("ping")

    def close(self):
        self.client.close()

    def get_database(self, name=None, **kwargs):
        return self.client.get_database(name, **kwargs)

    def get_collection(self, name, **kwargs):
        return self.db.get_collection(name, **kwargs)

    def create_index(self, coll_name, keys, unique=True, **kwargs):
        if isinstance(keys, str) or not isinstance(keys, Sequence) or not keys:
            raise ConfigurationError("Index keys must be a nonempty list of fields or (field, direction) pairs")
        normalized = []
        for key in keys:
            if isinstance(key, str):
                field, direction = key, ASCENDING
            elif isinstance(key, (tuple, list)) and len(key) == 2:
                field, direction = key
            else:
                raise ConfigurationError("Invalid MongoDB index key")
            if not isinstance(field, str) or not field or field.startswith("$"):
                raise ConfigurationError("MongoDB index fields must be nonempty strings")
            normalized.append((field, direction))
        return self.get_collection(coll_name).create_index(normalized, unique=unique, **kwargs)

    def get_index(self, coll_name):
        return self.get_collection(coll_name).index_information()

    def find(self, coll_name, condition=None, limit=1, projection=None, **kwargs):
        cursor = self.get_collection(coll_name).find(condition or {}, projection, **kwargs)
        return list(cursor.limit(limit))

    def count(self, coll_name, condition=None, **kwargs):
        return self.get_collection(coll_name).count_documents(condition or {}, **kwargs)

    def add(self, coll_name, data: Mapping[str, Any]):
        return self.get_collection(coll_name).insert_one(dict(data))

    def add_batch(self, coll_name, datas, **kwargs):
        return self.get_collection(coll_name).insert_many([dict(data) for data in datas], **kwargs)

    def update(self, coll_name, data, condition, upsert=False):
        return self.get_collection(coll_name).update_one(condition, {"$set": dict(data)}, upsert=upsert)

    def update_many(self, coll_name, data, condition, upsert=False):
        return self.get_collection(coll_name).update_many(condition, {"$set": dict(data)}, upsert=upsert)

    def delete(self, coll_name, condition):
        return self.get_collection(coll_name).delete_one(condition)

    def run_command(self, command):
        return self.db.command(command)

    def drop_collection(self, coll_name):
        return self.db.drop_collection(coll_name)

    def __getitem__(self, name):
        return self.get_collection(name)

    def __getattr__(self, name):
        # Preserve direct access to Database methods and collections.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self.db, name)

"""Isolated databases; never connect tests to business queues or collections."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import time
import uuid

import pytest
import redis


@pytest.fixture(scope="session")
def redis_socket_shared():
    binary = shutil.which("redis-server")
    if not binary:
        pytest.skip("redis-server is required for integration tests")
    with tempfile.TemporaryDirectory(prefix="sm-test-", dir="/tmp") as directory:
        socket_path = str(Path(directory) / "redis.sock")
        process = subprocess.Popen([binary, "--port", "0", "--unixsocket", socket_path, "--save", "",
                                    "--appendonly", "no", "--dir", directory],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        client = redis.Redis(unix_socket_path=socket_path, decode_responses=True)
        try:
            for _ in range(200):
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    time.sleep(0.01)
            else:
                pytest.fail("Isolated Redis did not start")
            yield socket_path
        finally:
            client.close()
            process.terminate()
            process.wait(timeout=10)


@pytest.fixture
def redis_client(redis_socket_shared):
    client = redis.Redis(unix_socket_path=redis_socket_shared, decode_responses=True)
    client.flushdb()
    yield client
    client.close()


@pytest.fixture
def real_mongo():
    uri = os.environ.get("SPIDER_MAN_TEST_MONGO_URI")
    if not uri:
        pytest.skip("Set SPIDER_MAN_TEST_MONGO_URI for real MongoDB integration tests")
    from spider_man.db.mongodb import MongoDB
    database = "spider_man_test_" + uuid.uuid4().hex
    mongo = MongoDB(url=uri, db=database, retryWrites=False, serverSelectionTimeoutMS=5000)
    mongo.ping()
    try:
        yield mongo
    finally:
        mongo.client.drop_database(database)
        mongo.close()

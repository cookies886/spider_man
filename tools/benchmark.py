#!/usr/bin/env python3
"""Disposable local two-process HTTP -> Redis -> MongoDB capacity check.

Creates an isolated Redis server and a random Mongo database, then removes both.
This is not a two-host acceptance test or a two-hour soak. The default workload
is 5,000 initial Seeds; every page-one Seed derives a page-two Seed, giving
10,000 Seed executions, HTTP requests, and confirmed unique Mongo documents.
Only a fully verified run writes its report. Partial runs print their counts.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
from pathlib import Path
import platform
import queue
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pymongo
import redis

from spider_man import Item, Crawler, Request, Seed
from spider_man.core.queue import RedisQueue


def worker(redis_url, mongo_uri, database, project, http_url, results):
    """Spawn target: create database clients only inside this child process."""
    app = None
    started = time.monotonic()
    try:
        app = Crawler(project, "posts", custom_settings={
            "REDIS_URL": redis_url, "MONGO_URI": mongo_uri, "MONGO_DATABASE": database,
            "SEED_CONCURRENCY": 32, "REQUEST_CONCURRENCY": 32,
            "ITEM_BATCH_SIZE": 64, "ITEM_BUFFER_LIMIT": 512,
            "ITEM_FLUSH_INTERVAL": 0.02, "REQUEST_BUFFER_LIMIT": 256,
            "REQUEST_MAX_RETRIES": 1, "REQUEST_TIMEOUT": 10.0,
            "LEASE_TIMEOUT": 30.0, "LEASE_RENEW_INTERVAL": 5.0,
            "RECOVERY_SCAN_INTERVAL": 0.2, "POLL_INTERVAL": 0.005,
            "METRICS_INTERVAL": 5.0, "SHUTDOWN_TIMEOUT": 2.0,
            "LOG_LEVEL": "CRITICAL",
        })

        @app.on_start
        def indexes():
            app.mongo.create_index(app.collection_name, ["user_id", "page"], unique=True)

        @app.request
        def request(seed):
            yield Request(http_url, params={"user_id": seed.user_id, "page": seed.page}, seed=seed)

        @app.parse
        def parse(response):
            body = response.json()
            for post in body["posts"]:
                yield Item(post, seed=response.seed)
            if body["next_page"] is not None:
                yield Seed({"user_id": response.seed.user_id, "page": body["next_page"]})

        snapshot = app.start(until_idle=True, idle_timeout=0.2)
        results.put({"worker_id": app.worker_id, "metrics": snapshot,
                     "elapsed_seconds": time.monotonic() - started})
    except BaseException as error:
        results.put({"error_type": type(error).__name__, "error": str(error)})
        raise
    finally:
        if app is not None:
            app.close()


def _free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def run(args):
    began = time.monotonic()
    # Reserve cleanup time so a failed workload cannot continue indefinitely.
    deadline = began + args.timeout - 8
    token = uuid4().hex[:12]
    project, database = "benchmark_" + token, "spider_man_benchmark_" + token
    collection = project + "_posts"
    mongo = pymongo.MongoClient(args.mongo_uri, serverSelectionTimeoutMS=3000,
                               connectTimeoutMS=3000, socketTimeoutMS=3000,
                               retryWrites=False)
    mongo.admin.command("ping")
    mongo_version = mongo.server_info()["version"]
    requests_seen = 0
    request_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *unused):
            pass

        def do_GET(self):
            nonlocal requests_seen
            values = parse_qs(urlsplit(self.path).query)
            user_id, page = int(values["user_id"][0]), int(values["page"][0])
            payload = json.dumps({"posts": [{"user_id": user_id, "page": page,
                                             "title": f"user-{user_id}-page-{page}"}],
                                  "next_page": 2 if page == 1 else None},
                                 separators=(",", ":")).encode()
            with request_lock:
                requests_seen += 1
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(payload)
            self.close_connection = True

    class Server(ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

    server = Server(("127.0.0.1", 0), Handler)
    http_thread = threading.Thread(target=server.serve_forever, daemon=True)
    http_thread.start()
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    processes = []
    redis_process = None
    client = None
    expected = args.users * 2
    report = None
    cleanup_complete = False
    try:
        with tempfile.TemporaryDirectory(prefix="sm-benchmark-", dir="/tmp") as directory:
            port = _free_port()
            redis_process = subprocess.Popen(
                [shutil.which("redis-server"), "--bind", "127.0.0.1", "--port", str(port),
                 "--save", "", "--appendonly", "no", "--dir", directory],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            redis_url = f"redis://127.0.0.1:{port}/0"
            client = redis.Redis.from_url(redis_url, decode_responses=True,
                                          socket_connect_timeout=1, socket_timeout=1)
            for _ in range(200):
                try:
                    if client.ping():
                        break
                except redis.ConnectionError:
                    if redis_process.poll() is not None:
                        raise RuntimeError("Disposable Redis exited during startup")
                    time.sleep(0.01)
            else:
                raise RuntimeError("Disposable Redis did not start")
            redis_version = client.info("server")["redis_version"]
            task_queue = RedisQueue(client, project, "posts")
            enqueue_started = time.monotonic()
            for user in range(args.users):
                if time.monotonic() >= deadline:
                    raise TimeoutError("Deadline reached while preparing seeds")
                task_queue.enqueue(Seed({"user_id": user, "page": 1}))
            enqueue_seconds = time.monotonic() - enqueue_started
            consumption_started = time.monotonic()
            for _ in range(2):
                process = context.Process(target=worker, args=(
                    redis_url, args.mongo_uri, database, project,
                    f"http://127.0.0.1:{server.server_port}/posts", results))
                process.start()
                processes.append(process)
            peak_processing = peak_buffer = 0
            next_progress = time.monotonic() + 10
            timed_out = False
            while any(process.is_alive() for process in processes):
                if time.monotonic() >= deadline:
                    timed_out = True
                    break
                counts = task_queue.counts()
                peak_processing = max(peak_processing, counts["processing"])
                heartbeats = task_queue.workers()
                peak_buffer = max(peak_buffer, sum(w.get("buffer_size", 0) for w in heartbeats))
                if time.monotonic() >= next_progress:
                    print(json.dumps({"elapsed_seconds": round(time.monotonic() - began, 2),
                                      "queue": counts, "http_requests": requests_seen}), flush=True)
                    next_progress = time.monotonic() + 10
                time.sleep(0.1)
            if timed_out:
                for process in processes:
                    if process.is_alive():
                        process.kill()
            for process in processes:
                process.join(timeout=1)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=1)
            consumption_seconds = time.monotonic() - consumption_started
            worker_reports = []
            while len(worker_reports) < 2:
                try:
                    worker_reports.append(results.get(timeout=0.2))
                except queue.Empty:
                    break
            counters = Counter()
            for result in worker_reports:
                counters.update(result.get("metrics", {}).get("counters", {}))
            final_counts = task_queue.counts()
            documents = mongo[database][collection].count_documents({})
            complete = (not timed_out and len(worker_reports) == 2
                        and all(process.exitcode == 0 for process in processes)
                        and all(count == 0 for count in final_counts.values())
                        and documents == expected and requests_seen == expected
                        and counters["seed_succeeded"] == expected
                        and counters["seed_claimed"] == expected
                        and counters["request_success"] == expected
                        and counters["item_inserted"] == expected)
            report = {
                "completed": complete,
                "recorded_at": datetime.now(timezone(timedelta(hours=8))).isoformat(),
                "scope": "local two spawned Worker processes; real HTTP, isolated real Redis, real MongoDB",
                "limitations": ["Not a two-host deployment", "Not a two-hour soak",
                                "Loopback synthetic HTTP; no proxy, failure injection or Redis failover",
                                "Redis persistence disabled for disposable benchmark; no persistence acceptance"],
                "workload": {"initial_seeds": args.users, "derived_page_seeds": args.users,
                             "expected_seed_executions": expected, "items_per_seed": 1,
                             "workers": 2, "seed_concurrency_per_worker": 32,
                             "request_concurrency_per_worker": 32,
                             "item_batch_size": 64, "item_buffer_limit_per_worker": 512},
                "environment": {"python": platform.python_version(), "system": platform.platform(),
                                "redis": redis_version, "redis_py": redis.__version__,
                                "mongodb": mongo_version, "pymongo": pymongo.version,
                                "mongo_endpoint": "127.0.0.1:" + str(urlsplit(args.mongo_uri).port or 27017)},
                "results": {"timed_out": timed_out, "worker_exit_codes": [p.exitcode for p in processes],
                            "confirmed_mongo_documents": documents, "http_requests": requests_seen,
                            "queue": final_counts, "counters": dict(counters),
                            "consumption_seconds": round(consumption_seconds, 3),
                            "seed_executions_per_second": round(expected / consumption_seconds, 2) if complete else None,
                            "enqueue_seconds": round(enqueue_seconds, 3),
                            "observed_peak_processing": peak_processing,
                            "observed_peak_buffer_from_heartbeats": peak_buffer},
                "worker_reports": worker_reports,
            }
    finally:
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join(timeout=1)
        if client is not None:
            client.close()
        if redis_process is not None:
            redis_process.terminate()
            redis_process.wait(timeout=2)
        server.shutdown()
        server.server_close()
        http_thread.join(timeout=1)
        # Only the randomly generated disposable database is removed.
        mongo.drop_database(database)
        mongo.close()
        results.close()
        cleanup_complete = True
    if report is not None:
        report["cleanup"] = {"temporary_redis_stopped": cleanup_complete,
                             "temporary_mongo_database_dropped": cleanup_complete,
                             "temporary_http_server_stopped": cleanup_complete}
        report["results"]["total_seconds_including_cleanup"] = round(time.monotonic() - began, 3)
        print(json.dumps(report, ensure_ascii=False), flush=True)
        if report["completed"]:
            output = Path(args.output)
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            return 0
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27183")
    parser.add_argument("--users", type=int, default=5000)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", default=str(Path(__file__).resolve().parents[1] / "docs/benchmark-20261011.json"))
    args = parser.parse_args()
    parsed = urlsplit(args.mongo_uri)
    if parsed.scheme != "mongodb" or parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
        parser.error("Benchmark MongoDB must be a loopback server")
    if args.users < 1 or not 10 <= args.timeout <= 180:
        parser.error("users must be positive and timeout between 10 and 180 seconds")
    if shutil.which("redis-server") is None:
        parser.error("redis-server is required")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())

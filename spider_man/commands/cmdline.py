"""CLI operates only on an explicitly imported Crawler and bounded batches."""
import argparse
import importlib
import importlib.util
import json
from pathlib import Path
import sys

from spider_man.network.seed import Seed
from spider_man.exceptions import ConfigurationError
from spider_man.core.crawler import Crawler
from spider_man.utils.log import safe_error


def load_app(reference, overrides=None):
    # Console entry points must be able to import the current business project.
    cwd = str(Path.cwd())
    if cwd not in sys.path:
        sys.path.insert(0, cwd)
    module_name, separator, attribute = reference.rpartition(":")
    if not separator or not module_name or not attribute:
        raise ConfigurationError("Crawler reference must be module:app or path.py:app")
    if module_name.endswith(".py"):
        path = Path(module_name).resolve()
        spec = importlib.util.spec_from_file_location("_spider_man_task", path)
        if spec is None or spec.loader is None:
            raise ConfigurationError("Cannot load task file")
        sys.path.insert(0, str(path.parent))
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    else:
        module = importlib.import_module(module_name)
    app = getattr(module, attribute)
    if not isinstance(app, Crawler):
        raise ConfigurationError("Selected object must be a Crawler")
    if overrides:
        app.settings = app.settings.with_cli(overrides)
        import threading
        app._http_slots = threading.BoundedSemaphore(app.settings["REQUEST_CONCURRENCY"])
    return app


def parser():
    root = argparse.ArgumentParser(prog="spider-man", description="spider_man Worker and queue tools")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("run", "seed", "status", "failed", "retry", "check"):
        command = commands.add_parser(name)
        command.add_argument("crawler", help="module:app or path.py:app")
        if name == "run":
            command.add_argument("--seed-concurrency", type=int)
            command.add_argument("--request-concurrency", type=int)
            command.add_argument("--until-idle", action="store_true")
            command.add_argument("--max-seeds", type=int)
        if name == "seed":
            command.add_argument("file", help="JSONL file (one business dictionary or versioned envelope per line)")
        if name in ("failed", "retry"):
            command.add_argument("--seed-id")
            command.add_argument("--error-type")
            command.add_argument("--limit", type=int, default=20)
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    app = None
    try:
        overrides = {key.upper(): value for key, value in vars(args).items()
                     if key in ("seed_concurrency", "request_concurrency") and value is not None}
        app = load_app(args.crawler, overrides)
        if args.command == "run":
            result = app.start(until_idle=args.until_idle, max_seeds=args.max_seeds)
        elif args.command == "check":
            result = app.check()
        else:
            app._connect_redis()
            if args.command == "status":
                result = {"project": app.project, "task": app.task, "queue": app.queue.counts(),
                          "workers": app.queue.workers(), "recent_errors": app.queue.failed(limit=5)}
            elif args.command == "seed":
                counts = {"accepted": 0, "duplicate": 0, "rejected": 0}
                errors = []
                with open(args.file, encoding="utf-8") as source:
                    for line_number, line in enumerate(source, 1):
                        if not line.strip():
                            continue
                        try:
                            value = json.loads(line)
                            seed = Seed.from_json(line) if isinstance(value, dict) and "version" in value and "seed_id" in value else Seed(value)
                            counts["accepted" if app.enqueue(seed) else "duplicate"] += 1
                        except (ValueError, TypeError) as exc:
                            counts["rejected"] += 1
                            if len(errors) < 20:
                                errors.append({"line": line_number, "error": safe_error(exc)})
                result = {**counts, "errors": errors}
            else:
                if not 1 <= args.limit <= 1000:
                    raise ConfigurationError("--limit must be between 1 and 1000")
                failures = app.queue.failed(limit=args.limit, error_type=args.error_type, seed_id=args.seed_id)
                if args.command == "failed":
                    result = failures
                else:
                    replays = []
                    for failure in failures:
                        replay = app.queue.replay(failure["seed_id"])
                        if replay:
                            replays.append({"original_seed_id": failure["seed_id"], "replay_seed_id": replay.seed_id})
                    result = {"replayed": len(replays), "records": replays, "original_failures_retained": True,
                              "dedup_window_seconds": app.settings["DEDUP_TTL"],
                              "notice": "Original child identities are reused within DEDUP_TTL; descendants may run again after retention expires. Inserts without business unique keys may duplicate data."}
        print(json.dumps(result, ensure_ascii=False, default=str))
        return 1 if args.command == "check" and not result["ready"] else 0
    except (Exception, KeyboardInterrupt) as exc:
        print(json.dumps({"error_type": type(exc).__name__, "error": safe_error(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    finally:
        if app is not None:
            app.close()

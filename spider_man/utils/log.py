"""Structured task logs; secrets in exception text are summarized."""
import json
import logging
import re
from datetime import datetime, timezone


def safe_error(error):
    text = str(error)
    text = re.sub(r"(?:https?|mongodb(?:\+srv)?|rediss?)://[^\s'\"]+", "[connection redacted]", text)
    text = re.sub(r"(?i)(authorization|cookie|password|token|secret)(\s*[:=]\s*)[^\s,;]+", r"\1\2[redacted]", text)
    return text[:1500]


class TaskLogger:
    def __init__(self, project, task, worker_id, settings):
        self.tags = dict(project=project, task=task, worker_id=worker_id)
        self.json = settings["LOG_JSON"]
        self.logger = logging.getLogger(f"spider_man.{worker_id}")
        self.logger.setLevel(settings["LOG_LEVEL"])
        self.logger.propagate = False
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)

    def event(self, name, *, level=logging.INFO, **fields):
        body = {"timestamp": datetime.now(timezone.utc).isoformat(), "event": name, **self.tags, **fields}
        self.logger.log(level, json.dumps(body, ensure_ascii=False, default=str) if self.json else str(body))

"""Log items for local debugging without claiming durable persistence."""

import json
import logging

from spider_man.pipelines.base_pipeline import BasePipeline

log = logging.getLogger(__name__)


class ConsolePipeline(BasePipeline):
    required = False
    persistent = False

    def __init__(self, app):
        self.app = app

    def open(self):
        pass

    def process_item(self, item):
        log.info("console_item %s", json.dumps(dict(item.data), ensure_ascii=False, default=str))
        return item

    def flush(self):
        pass

    def close(self):
        pass

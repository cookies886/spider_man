"""Optional Pipeline extension contract and confirmed item outcomes.

Pipelines may also be duck-typed plugins. A persistent pipeline must return an
explicit PipelineResult for every confirmed item. Batch-only plugins implement
process_batch and return one result or exception per input, in input order.
"""

from __future__ import annotations

from dataclasses import dataclass

_STATUSES = frozenset({"inserted", "updated", "unchanged", "dropped"})


class BasePipeline:
    """Optional base for item plugins with Crawler-owned lifecycle hooks.

    Implement process_item or process_batch. process_batch deliberately has no
    default implementation so single-item plugins retain manager retry behavior.
    """

    required = True
    persistent = False

    def __init__(self, app):
        self.app = app

    def open(self):
        pass

    def process_item(self, item):
        raise NotImplementedError

    def flush(self):
        pass

    def close(self):
        pass


@dataclass(frozen=True)
class PipelineResult:
    status: str
    reason: str | None = None

    def __post_init__(self):
        if self.status not in _STATUSES:
            raise ValueError(f"Unsupported PipelineResult status: {self.status}")
        if self.status == "dropped" and (not isinstance(self.reason, str) or not self.reason.strip()):
            raise ValueError("Dropping an item requires a nonempty business reason")


class PipelineBatchError(Exception):
    """A custom batch pipeline can retain individual confirmations on failure."""

    def __init__(self, results):
        super().__init__("Pipeline batch contains unsuccessful items")
        self.results = results

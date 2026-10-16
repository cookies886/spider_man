"""Item pipeline contracts and persistence adapters."""

from spider_man.pipelines.base_pipeline import BasePipeline, PipelineBatchError, PipelineResult
from spider_man.pipelines.console_pipeline import ConsolePipeline
from spider_man.pipelines.mongo_pipeline import MongoPipeline

__all__ = [
    "BasePipeline",
    "ConsolePipeline",
    "MongoPipeline",
    "PipelineBatchError",
    "PipelineManager",
    "PipelineResult",
]


def __getattr__(name):
    if name == "PipelineManager":
        from spider_man.buffer.item_buffer import PipelineManager
        return PipelineManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

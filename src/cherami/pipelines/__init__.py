from importlib import import_module
from types import ModuleType

from cherami.pipelines.pipeline import Pipeline  # noqa: F401
from cherami.pipelines.worker import Worker  # noqa: F401


def load_pipeline_module(pipeline_name: str) -> ModuleType:
    module_name = pipeline_name.replace("-", "_")
    return import_module(f"cherami.pipelines.{module_name}")

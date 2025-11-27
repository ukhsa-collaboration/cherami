from cherami.pipelines.amr import AmrPipeline
from cherami.pipelines.orange_box import OrangeBoxPipeline
from cherami.pipelines.pipeline import Pipeline  # noqa: F401

PIPELINES: dict[str, type[Pipeline]] = {
    "amr": AmrPipeline,
    "orange_box": OrangeBoxPipeline,
}

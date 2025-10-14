from cherami.pipelines import implementations
from cherami.pipelines.base import Pipeline

PIPELINES: dict[str, type[Pipeline]] = {
    "amr": implementations.AmrPipeline,
    "orange_box": implementations.OrangeBoxPipeline,
    "sarscov2": implementations.SARSCoV2Pipeline,
}

__all__ = ["implementations", "PIPELINES", "Pipeline"]

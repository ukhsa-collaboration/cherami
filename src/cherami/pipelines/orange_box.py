import logging
from pathlib import Path

from cherami.pipelines.pipeline import Pipeline

logger = logging.getLogger(__name__)


class OrangeBoxPipeline(Pipeline):
    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        # TODO
        return

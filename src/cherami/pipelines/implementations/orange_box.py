import logging
from pathlib import Path

from cherami.pipelines.base import Pipeline, PipelineConfig

logger = logging.getLogger(__name__)


class OrangeBoxPipeline(Pipeline):
    pipeline_name = "orange-box"

    @property
    def config(self) -> PipelineConfig:
        return PipelineConfig(
            name=self.pipeline_name,
            version="0.1.0",
            path="/shared/team/projects/downstream_orchestration/orange-box",
            cpus=4,
            mem="8G",
            cpu_limit=4,
            mem_limit="8G",
            nf_config_path=Path("/shared/team/projects/downstream_orchestration/orange-box/nextflow.config"),
            nf_profiles=["synthscape", "docker"],
            nf_extra_args=[],
            work_dir=Path("/shared/team/projects/downstream_orchestration/orange_box/work"),
            output_dir=Path("/shared/team/projects/downstream_orchestration/orange_box/output"),
            namespace="ns-synthscape-ukhsa",
            container="quay.io/climb-tre/nextflow",
            backoff_limit=5,
            max_retries=1,
            retry_timeout=10,
            job_timeout=3600,
        )

    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        # TODO
        return

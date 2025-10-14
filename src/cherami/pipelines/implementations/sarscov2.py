import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from onyx import OnyxClient

from cherami.pipelines.base import Pipeline, PipelineConfig
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


class SARSCoV2Pipeline(Pipeline):
    pipeline_name = "sarscov2"

    def __init__(self) -> None:
        self._onyx_config = init_onyx()
        self._pipeline_criteria: dict[str, dict[str, Any]] = {
            self.pipeline_name: {
                "min_total_reads": 10,
                "target_taxa": {694009: 1000},
            }
        }

    @property
    def config(self) -> PipelineConfig:
        return PipelineConfig(
            name=self.pipeline_name,
            version="0.1.0",
            path="nf-core/demo",
            cpus=4,
            mem="8G",
            cpu_limit=4,
            mem_limit="8G",
            nf_config_path=Path("/shared/team/projects/downstream_orchestration/nextflow.config"),
            nf_profiles=["docker", "test"],
            nf_extra_args=[],
            work_dir=Path("/shared/team/projects/downstream_orchestration/test/work"),
            output_dir=Path("/shared/team/projects/downstream_orchestration/test/output"),
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

    def _meets_criteria(
        self,
        total_reads: int,
        taxon_reads: dict[int, int],
        criteria: dict[str, Any],
    ) -> bool:
        if total_reads < criteria["min_total_reads"]:
            logger.debug(
                "Sample total reads %d below minimum %d",
                total_reads,
                criteria["min_total_reads"],
            )
            return False

        for taxon_id, min_reads in criteria["target_taxa"].items():
            if taxon_reads.get(taxon_id, 0) < min_reads:
                logger.debug(
                    "Sample missing reads for taxon %d: %d < %d",
                    taxon_id,
                    taxon_reads.get(taxon_id, 0),
                    min_reads,
                )
                return False

        return True

    def should_run(self, sample_id: str) -> bool:
        criteria = self._pipeline_criteria.get(self.pipeline_name)
        if not criteria:
            return True

        target_taxa = set(criteria["target_taxa"].keys())

        try:
            with OnyxClient(self._onyx_config) as client:
                record = client.get("synthscape", sample_id, include=["classifier_calls"])
        except Exception:
            logger.exception("Failed to fetch Onyx data for sample %s", sample_id)
            return False

        classifier_calls: Sequence[dict[str, Any]] = record.get("classifier_calls", [])

        taxon_reads = dict.fromkeys(target_taxa, 0)
        total_reads = 0
        for classifier_call in classifier_calls:
            count = classifier_call.get("count_descendants", 0) or 0
            total_reads += count
            taxon_id = classifier_call.get("taxon_id")
            if taxon_id in taxon_reads:
                taxon_reads[taxon_id] = count

        return self._meets_criteria(total_reads, taxon_reads, criteria)

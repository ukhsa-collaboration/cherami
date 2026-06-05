import csv
import logging
from pathlib import Path
from typing import Any

from cherami.config import CheramiConfig, GlobalConfig, PipelineConfig
from cherami.pipelines.pipeline import Pipeline, PipelineContext
from cherami.pipelines.worker import Worker

logger = logging.getLogger(__name__)


class OrangeBoxPipeline(Pipeline):
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        rows = []
        for climb_id in samples:
            row = {
                "climb_id": climb_id,
            }
            rows.append(row)
        if not rows:
            raise ValueError("samplesheet_generation_no_records")

        fieldnames = list(rows[0].keys())
        with output_filepath.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.debug(
            "Generated orange_box samplesheet at %s",
            output_filepath,
        )

    def build_context(self, payload: Any) -> PipelineContext:
        context = super().build_context(payload)
        context.set_upstream_context_hash()
        context.orange_box_version = self.config.version
        return context


def build_worker(
    config: CheramiConfig,
    work_dir: Path,
    output_dir: Path,
    audit_db_path: Path,
) -> Worker:
    pipeline = build_pipeline(config.pipeline_config, config.global_config)
    return Worker(
        config.worker_config,
        pipeline,
        work_dir,
        output_dir,
        audit_db_path=audit_db_path,
    )


def build_pipeline(
    pipeline_config: PipelineConfig, global_config: GlobalConfig
) -> Pipeline:
    return OrangeBoxPipeline(pipeline_config, global_config)

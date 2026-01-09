import csv
import logging
from pathlib import Path

from onyx import OnyxClient

from cherami.config import PipelineConfig, WorkerConfig
from cherami.pipelines.pipeline import Pipeline
from cherami.pipelines.worker import Worker
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


class AmrPipeline(Pipeline):
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        config = init_onyx()
        rows = []
        with OnyxClient(config) as client:
            for climb_id in samples:
                climb_records = list(
                    client.filter(project="synthscape", climb_id=climb_id)
                )
                record = climb_records[0]
                read1_fastq = record["human_filtered_reads_1"]
                read_2_fastq = record["human_filtered_reads_2"]
                taxon_reports = record["taxon_reports"]
                row = {
                    "climb_id": climb_id,
                    "human_filtered_reads_1": read1_fastq,
                    "human_filtered_reads_2": read_2_fastq,
                    "taxon_reports": taxon_reports,
                }
                rows.append(row)

        if not rows:
            raise ValueError("Samplesheet generation produced no records")

        fieldnames = list(rows[0].keys())
        with output_filepath.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.debug(
            "Generated AMR samplesheet at %s",
            output_filepath,
        )


def build_worker(
    worker_config: WorkerConfig,
    pipeline_config: PipelineConfig,
    work_dir: Path,
    output_dir: Path,
) -> Worker:
    pipeline = AmrPipeline(pipeline_config)
    return Worker(worker_config, pipeline, work_dir, output_dir)

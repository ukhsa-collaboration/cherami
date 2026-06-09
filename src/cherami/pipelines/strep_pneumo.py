import csv
import logging
from functools import cache
from pathlib import Path

from onyx import OnyxClient, OnyxConfig

from cherami.config import CheramiConfig, GlobalConfig, PipelineConfig
from cherami.pipelines.pipeline import PathCharPipeline, PipelineContext
from cherami.pipelines.worker import Worker
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


@cache
def onyx_config() -> OnyxConfig:
    return init_onyx()


class StrepPneumoPipeline(PathCharPipeline):
    @property
    def proc_names(self) -> dict[str, list[int]]:
        """Optional mapping of Nextflow process names to their allowed exit codes.

        Returns:
            Process-specific allowed exit codes used when evaluating trace files. When
            empty, every process must exit with code 0.
        """
        return {
            # Placeholders - add additional allowed exit codes/behaviour when implemented
            "run_kractor": [0],
            "run_pneumokity": [0],
            "add_pneumokity_results_onyx": [0],
        }

    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        rows = []
        with OnyxClient(onyx_config()) as client:
            for climb_id in samples:
                climb_records = client.get(
                    project="synthscape",
                    climb_id=climb_id,
                    include=[
                        "human_filtered_reads_1",
                        "human_filtered_reads_2",
                        "taxon_reports",
                    ],
                )
                if not climb_records:
                    raise ValueError("no_records_found")
                try:
                    # Pneumokity requires 2 fastqs as input so for SE pass same fastq twice
                    if climb_records["human_filtered_reads_2"] == "":
                        climb_records["human_filtered_reads_2"] = (
                            climb_records["human_filtered_reads_1"]
                        )
                    row = {
                        "climb_id": climb_id,
                        "fastq_1": climb_records["human_filtered_reads_1"],
                        "fastq_2": climb_records["human_filtered_reads_2"],
                        "kraken_output": f"{climb_records['taxon_reports']}{climb_id}_PlusPF.kraken_assignments.tsv",
                        "kraken_report": f"{climb_records['taxon_reports']}{climb_id}_PlusPF.kraken_report.txt",
                    }
                except KeyError as e:
                    raise ValueError(
                        f"missing_expected_data: {e.args[0]}"
                    ) from e
                rows.append(row)

        if not rows:
            raise ValueError("samplesheet_generation_no_records")

        fieldnames = list(rows[0].keys())
        with output_filepath.open("w") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.debug(
            "Generated Strep pneumo samplesheet at %s",
            output_filepath,
        )

    def should_run(self, context: PipelineContext) -> bool:
        """Determine whether the Strep pneumo pipeline should run for the given sample.
        When this returns False, the worker calls `on_skip()` instead of launching the pipeline.

        Arguments:
            context: PipleineContext object that contains key information like
            sample id and server.

        Returns:
            `True` when the pipeline should run, otherwise `False`.
        """
        # Get classifer calls info from onyx:
        with OnyxClient(onyx_config()) as client:
            climb_records = client.get(
                project=context.server,
                climb_id=context.climb_id,
                include=[
                    "classifier_calls__taxon_id",
                    "classifier_calls__count_descendants",
                ],
            )
        # Set criteria for pipeline running - currently 100 reads of Strep pneumo
        strep_pneumo_taxon_id = 1313
        min_descendant_reads = 100
        strep_finder = (
            taxa_dict
            for taxa_dict in climb_records["classifier_calls"]
            if (taxa_dict.get("taxon_id") == strep_pneumo_taxon_id)
            and (taxa_dict.get("count_descendants") >= min_descendant_reads)
        )
        # Iterate through list of dicts - return taxon_dict if taxon present, None if taxon not present
        strep_present = next(strep_finder, None)

        if bool(strep_present):
            should_run_response = super().should_run(context)
            return should_run_response
        else:
            return False


def build_worker(
    config: CheramiConfig,
    work_dir: Path,
    output_dir: Path,
    audit_db_path: Path,
) -> Worker:
    pipeline: PathCharPipeline = build_pipeline(
        config.pipeline_config, config.global_config
    )
    return Worker(
        config.worker_config,
        pipeline,
        work_dir,
        output_dir,
        audit_db_path=audit_db_path,
    )


def build_pipeline(
    pipeline_config: PipelineConfig, global_config: GlobalConfig
) -> PathCharPipeline:
    return StrepPneumoPipeline(pipeline_config, global_config)

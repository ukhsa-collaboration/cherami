import csv
import logging
from functools import cache
from pathlib import Path

from onyx import OnyxClient, OnyxConfig

from cherami.config import CheramiConfig, GlobalConfig, PipelineConfig
from cherami.pipelines.pipeline import (
    PathCharPipeline,
    PipelineContext,
    get_context_from_record,
)
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
        self,
        samples: list[str],
        job_id: str,
        output_filepath: Path,
        context: PipelineContext,
    ) -> None:
        """
        Custom samplesheet constructor. Includes the orange box version from
        the context object.

        Raises:
            ValueError - if climb id not found in onyx.
        """
        rows = []
        with OnyxClient(onyx_config()) as client:
            for sample_id in samples:
                sample_record = client.get(
                    project="synthscape",
                    climb_id=sample_id,
                    include=[
                        "human_filtered_reads_1",
                        "human_filtered_reads_2",
                        "taxon_reports",
                    ],
                )
            if not sample_record:
                raise ValueError("No records found for sample %s.", sample_id)

            try:
                # Pneumokity requires 2 fastqs as input so for SE pass same fastq twice
                if sample_record["human_filtered_reads_2"] == "":
                    sample_record["human_filtered_reads_2"] = sample_record[
                        "human_filtered_reads_1"
                    ]
                row = {
                    "climb_id": sample_id,
                    "fastq_1": sample_record["human_filtered_reads_1"],
                    "fastq_2": sample_record["human_filtered_reads_2"],
                    "kraken_output": f"{sample_record['taxon_reports']}{sample_id}_PlusPF.kraken_assignments.tsv",
                    "kraken_report": f"{sample_record['taxon_reports']}{sample_id}_PlusPF.kraken_report.txt",
                    "orange_box_version": context.orange_box_version,
                }
            except KeyError as e:
                raise ValueError(f"Missing expected field: {e.args[0]}") from e
            rows.append(row)

        if not rows:
            raise ValueError(
                "Samplesheet generator found no records for sample."
            )

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
        """
        Determine whether the Strep pneumo pipeline should run for the given
        sample.
        Pull the claspar Kraken outputs, and check whether Strep is present.
        When this returns False, the worker calls `on_skip()` instead of
        launching the pipeline.

        Arguments:
            context: PipleineContext object that contains key information like
            sample id and server.

        Returns:
            `True` when the pipeline should run, otherwise `False`.
        """
        strep_pneumo_taxon_id = 1313
        select_table_name = "claspar-kraken-bacteria"

        # Has the pipeline with the current context been run before?
        first_should_run = super().should_run(context)

        if not first_should_run:
            # if we have run before with a valid table for the pipeline,
            # don't run
            return False

        # 1) collate the current context from context object
        current_context = (
            context.onyx_versions_hash,
            context.orange_box_version,
        )

        # 2) get all the claspar analysis tables with name select_table_name
        # associated with the sample.
        from onyx_analysis_helper import onyx_analysis_helper_functions as oa

        analysis_tables: dict
        exitcode: int
        analysis_tables, exitcode = oa.get_analysis_records(
            sample_id=context.climb_id,
            server=context.server,
            fields=[
                "methods",
                "name",
                "result_metrics",
            ],
        )

        # Get specific claspar tables using analysis_id (aid) as key
        claspar_tables = {
            aid: table
            for aid, table in analysis_tables.items()
            if table["name"] == select_table_name
        }

        # 3.) get the claspar table that matches the current context
        for analysis_id, table in claspar_tables.items():
            try:
                onyx_versions_hash, orange_box_version = (
                    get_context_from_record(table, analysis_id)
                )
            except KeyError:
                # If get a table without onyx_versions_hash or
                # orange_box_version, just ignore and check the next table.
                continue

            if (onyx_versions_hash, orange_box_version) == current_context:
                # check the results:
                for result in table["result_metrics"].values():
                    if (
                        int(result["profile_taxon_id"])
                        == strep_pneumo_taxon_id
                        and result["kraken_confidence"] == "high"
                    ):
                        logger.debug(
                            "Incoming sample %s has claspar "
                            "analysis table (id: %s) with 'high' strep pneumo - "
                            "Decision: run",
                            context.climb_id,
                            analysis_id,
                        )
                        return True
                    elif (
                        int(result["profile_taxon_id"])
                        == strep_pneumo_taxon_id
                        and result["kraken_confidence"] == "low"
                    ):
                        logger.debug(
                            "Incoming sample %s has claspar "
                            "analysis table (id: %s) with 'low' strep pneumo - "
                            "Decision: not run",
                            context.climb_id,
                            analysis_id,
                        )
                        return False
        logger.debug(
            "Incoming sample %s has %s claspar kraken tables "
            "(ids: %s) without Strep pneumoniae. Decision: not run",
            context.climb_id,
            len(claspar_tables),
            list(claspar_tables.keys()),
        )
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

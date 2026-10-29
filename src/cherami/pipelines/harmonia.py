import csv
import logging
from functools import cache
from pathlib import Path

from onyx import OnyxClient, OnyxConfig

from cherami.pipeline_runner import NonRetryablePipelineError
from cherami.pipelines.pipeline import (
    PathCharPipeline,
    PipelineContext,
    get_context_from_record,
)
from cherami.utils import init_onyx

logger = logging.getLogger(__name__)


@cache
def onyx_config() -> OnyxConfig:
    return init_onyx()


class HarmoniaPipeline(PathCharPipeline):
    def generate_samplesheet(
        self,
        samples: list[str],
        job_id: str,
        output_filepath: Path,
        context: PipelineContext,
    ) -> None:
        """
        Custom samplesheet constructor. Includes the orange box version from
        the context object and the bam file.

        Raises:
            ValueError - if climb id not found in onyx.
        """
        rows = []
        with OnyxClient(onyx_config()) as client:
            for sample_id in samples:
                sample_record = client.get(
                    project=context.server,
                    climb_id=sample_id,
                    include=[
                        "chimera_bam",
                    ],
                )
            if not sample_record:
                raise ValueError("No records found for sample %s.", sample_id)

            try:
                row = {
                    "climb_id": sample_id,
                    "bam_file": sample_record["chimera_bam"],
                    "orange_box_version": context.orange_box_version,
                    "virus_taxids": "2697049",  # viruses - for now, just covid
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
            "Generated Harmonia samplesheet at %s",
            output_filepath,
        )

    def should_run(self, context: PipelineContext) -> bool:
        """
        Determine whether or not Harmonia should run for the given sample.
        Specifically check whether or not SARS-CoV-2 is present with high
        confidence in claspar outputs.

        Args:
            context (PipelineContext): PipleineContext object that contains key
            information like sample id and server.

        Returns:
            bool: `True` when the pipeline should run, otherwise `False`.
        """
        select_table_name = "claspar-viralaligner-virus"
        covid_taxon_id = 2697049

        # Has the pipeline with the current context been run before?
        first_should_run = super().should_run(context)

        if not first_should_run:
            # if we have run before with a valid table for the pipeline, don't
            # run
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
        viral_aligner_tables = {
            aid: table
            for aid, table in analysis_tables.items()
            if table["name"] == select_table_name
        }

        # If there are no claspar tables, raise:
        if len(viral_aligner_tables) == 0:
            logger.error(
                "No Claspar viral aligner analysis records found "
                "for sample %s",
                context.climb_id,
            )
            raise NonRetryablePipelineError("claspar_tables_not_found.")

        # 3.) get the claspar table that matches the current context
        for analysis_id, table in viral_aligner_tables.items():
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
                        int(result["profile_taxon_id"]) == covid_taxon_id
                        and result["kraken_confidence"] == "high"
                    ):
                        logger.debug(
                            "Incoming sample %s has claspar viral aligner"
                            "analysis table (id: %s) with 'high' SARS-CoV-2 - "
                            "Decision: run",
                            context.climb_id,
                            analysis_id,
                        )
                        return True
                    elif (
                        int(result["profile_taxon_id"]) == covid_taxon_id
                        and result["kraken_confidence"] == "low"
                    ):
                        logger.debug(
                            "Incoming sample %s has claspar viral aligner"
                            "analysis table (id: %s) with 'low' SARS-CoV-2 - "
                            "Decision: not run",
                            context.climb_id,
                            analysis_id,
                        )
                        return False
        logger.debug(
            "Incoming sample %s has %s claspar viral aligner tables "
            "(ids: %s) without SARS-CoV-2. Decision: not run",
            context.climb_id,
            len(viral_aligner_tables),
            list(viral_aligner_tables.keys()),
        )
        return False

import csv
import logging
from pathlib import Path

from onyx_analysis_helper import onyx_analysis_helper_functions as oa

from cherami.config import GlobalConfig, PipelineConfig, WorkerConfig
from cherami.pipelines.pipeline import Pipeline
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

    def should_run(self, sample_id: str) -> bool:
        """
        Determines whether orangebox should run.
        Orange Box should run for a sample when either:
        - no orangebox analysis tables exist for the current configured
        orangebox version.
        - the current Onyx upstream context differs from the upstream context
        stored on orangebox analysis tables for the current configured
        orangebox version.
        Arguments:
            sample_id: str, sample id.
            server: str, must be valid onyx server.
        Returns:
            should_run_decision: should run? - bool True for yes False for no.
        """
        analysis_tables, exitcode = oa.get_analysis_records(
            sample_id=sample_id,
            server=GlobalConfig.server,
            fields=["methods"],  # Need to set server
        )

        if exitcode != 0:
            logger.error(
                "Cannot query Onyx for analyses for sample %s." % (sample_id)  # noqa: G002, UP031
            )
            return False

        # First get a set of the orange_box_versions and the onyx_version_hashes
        orange_box_versions = set()
        onyx_versions_hashes = set()

        for table in analysis_tables.values():
            # add onyx versions hashes from analysis tables:
            onyx_versions_hashes.add(table["methods"]["onyx_versions_hash"])

            # Get the orange box version from the analysis tables
            versions = table["methods"]["versions"]
            versions_dict = {ver["name"]: ver["version"] for ver in versions}
            orange_box_versions.add(versions_dict.get("orange_box_version"))

        # Then get the current upstream context:
        _, current_onyx_versions, exitcode = (
            oa.get_data_and_versions_from_onyx(
                sample_id=sample_id,
                server=GlobalConfig.server,
                fields=["climb_id"],
            )
        )
        if exitcode != 0:
            logger.error(
                "Cannot query Onyx for upstream context, uncertain whether to run, exiting."
            )
            return False

        current_onyx_hash = oa._calculate_versions_hash(current_onyx_versions)

        # Get the orange box version:
        orange_box_version = PipelineConfig.version

        # Then check if all of the current onyx

        if (  # noqa: SIM103
            orange_box_version in orange_box_versions
            and current_onyx_hash in onyx_versions_hashes
        ):
            # do not rerun if orange box version matches and upstream context matches
            return False
        else:
            return True


def build_worker(
    worker_config: WorkerConfig,
    pipeline_config: PipelineConfig,
    work_dir: Path,
    output_dir: Path,
    audit_db_path: Path,
) -> Worker:
    pipeline = build_pipeline(pipeline_config)
    return Worker(
        worker_config,
        pipeline,
        work_dir,
        output_dir,
        audit_db_path=audit_db_path,
    )


def build_pipeline(pipeline_config: PipelineConfig) -> Pipeline:
    return OrangeBoxPipeline(pipeline_config)

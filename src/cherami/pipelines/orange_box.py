import csv
import logging
from pathlib import Path
from typing import Any

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
        if not self.current_onyx_hash:
            self.get_upstream_context_hash(sample_id)

        analysis_tables, exitcode = oa.get_analysis_records(
            sample_id=sample_id,
            server=GlobalConfig.server,
            fields=["methods"],  # Need to set server
        )

        if exitcode != 0:
            logger.error(
                "Cannot query Onyx for analyses for sample %s.", sample_id
            )
            return False

        # If there are no analysis tables, just run:
        if not analysis_tables:
            logger.info(
                "Inbound sample %s has no analysis tables, running orange box.",
                sample_id,
            )
            return True

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

        # Get the orange box version:
        orange_box_version = PipelineConfig.version

        # Then check if all of the current onyx
        orange_box_match = orange_box_version in orange_box_versions
        upstream_context_match = self.current_onyx_hash in onyx_versions_hashes

        if orange_box_match and upstream_context_match:
            # do not rerun if orange box version matches and upstream context matches
            logger.warning(
                "Sample %s has up-to-date analysis tables, skipping.",
                sample_id,
            )
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "are up-to-date - orange box version(s) %s, upstream context "
                "hash %s. Decision: not run.",
                sample_id,
                list(analysis_tables.keys()),
                list(orange_box_versions),
                self.current_onyx_hash,
            )
            return False
        elif upstream_context_match and not orange_box_match:
            # Orange box version does not match, rerun:
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "have different orange box version(s) - %s, current "
                "orange box version is %s. Upstream context in analysis "
                "tables matches current, hash: %s "
                "Decision: run.",
                sample_id,
                list(analysis_tables.keys()),
                list(orange_box_versions),
                orange_box_version,
                self.current_onyx_hash,
            )
            return True
        elif orange_box_match and not upstream_context_match:
            # upstream context does not match, some change in onyx versions, run
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "have orange box version(s) %s. Current "
                "upstream context has hash %s which does not match any "
                "analysis tables. Decision: run.",
                sample_id,
                list(analysis_tables.keys()),
                list(orange_box_versions),
                self.current_onyx_hash,
            )
            return True
        elif not orange_box_match and not upstream_context_match:
            # neither match
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "have different orange box version(s) - %s. Current "
                "upstream context has hash %s which does not match any "
                "analysis tables. Decision: run.",
                sample_id,
                list(analysis_tables.keys()),
                list(orange_box_versions),
                self.current_onyx_hash,
            )
            return True
        else:
            logger.debug(
                "Unexpected error with Orange Box should run decision logic."
            )
            return False


class OrangeBoxWorker(Worker):
    def on_skip(self, message: Any) -> None:
        """Handle messages that should be skipped.

        Orange box implementation will add the message to the publish queue
        before acknowledging the message to remove it from the incoming queue.

        Args:
            message: The Varys message object associated with the current
            sample.

        Raises:
            Exception: If the Varys client fails to acknowledge the message.
        """
        payload, _, _ = self._parse_message(message)
        # payload should store orange box version and onyx versions hash
        payload["upstream_onyx_hash"] = self.pipeline.current_onyx_hash
        payload["orange_box_version"] = PipelineConfig.version

        if self.publish_queue_suffix:
            self._varys_client.send(
                message=payload,
                exchange=self.publish_exchange,
                queue_suffix=self.publish_queue_suffix,
            )

        self._varys_client.acknowledge_message(message)


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

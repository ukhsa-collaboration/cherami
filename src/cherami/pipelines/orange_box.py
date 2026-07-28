import csv
import logging
from pathlib import Path
from typing import Any

from cherami.config import CheramiConfig, GlobalConfig, PipelineConfig
from cherami.pipelines.pipeline import (
    Pipeline,
    PipelineContext,
    get_context_from_record,
)
from cherami.pipelines.worker import Worker, WorkerError

logger = logging.getLogger(__name__)


class OrangeBoxPipeline(Pipeline):
    def generate_samplesheet(
        self,
        samples: list[str],
        job_id: str,
        output_filepath: Path,
        context: PipelineContext,
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
        """
        Instantiate the PipelineContext object, and add the
        onyx_versions_hash and the orange_box_version.

        Arguments:
            payload: dict, information from the message
        Returns:
            context: PipelineContext object.
        Raises:
            RuntimeError: if onyx cannot be reached.
            ValueError: if any of the required fields are not in the payload.
        """
        context: PipelineContext = super().build_context(payload)
        context.onyx_versions_hash = context.get_upstream_context_hash()
        context.orange_box_version = self.config.version
        return context

    def should_run(self, context: PipelineContext) -> bool:
        """
        Determines whether orangebox should run.
        Orange Box should run for a sample when either:
        - no orangebox analysis tables exist for the current configured
        orangebox version.
        - the current Onyx upstream context differs from the upstream context
        stored on orangebox analysis tables for the current configured
        orangebox version.
        Arguments:
            context: PipelineContext object, returned from build_context method
        Returns:
            should_run_decision: should run? - bool True for yes False for no.
        """
        # lazy import because oa needs env vars.
        from onyx_analysis_helper import onyx_analysis_helper_functions as oa

        # First get all the analysis tables associated with the sample. This
        # will include downstream analyses too, but that does not matter
        # because if there are any present, they must have an upstream orange
        # box analysis table to have run to create those tables.

        analysis_tables: dict[str | None, Any | None]
        exitcode: int
        analysis_tables, exitcode = oa.get_analysis_records(
            sample_id=context.climb_id,
            server=context.server,
            fields=["methods", "analysis_id"],
        )

        # If we cannot get to onyx, exit early
        if exitcode != 0:
            logger.error(
                "Cannot query Onyx for analyses for sample %s.",
                context.climb_id,
            )
            raise RuntimeError("Cannot query onyx - check logs for reasons.")

        # If there are no analysis tables, just run:
        if not analysis_tables:
            logger.debug(
                "Inbound sample %s has no analysis tables, running orange box.",
                context.climb_id,
            )
            return True

        # Get a set of the (orange_box_version, onyx_version_hash) in all
        # analysis tables - this is the state of onyx + orange box versions run
        # previously
        upstream_contexts: set[tuple] = set()

        for analysis_id, table in analysis_tables.items():
            try:
                onyx_versions_hash, orange_box_version = (
                    get_context_from_record(table, analysis_id)
                )
            except KeyError:
                # If get a table without onyx_versions_hash or
                # orange_box_version, just ignore and check the next table.
                continue

            upstream_contexts.add((onyx_versions_hash, orange_box_version))

        # Make the current tuple to compare - this is the state of Onyx and
        # orange box version now at time of running:
        current_context: tuple[str | Any, str | Any] = (
            context.onyx_versions_hash,
            context.orange_box_version,
        )

        if current_context in upstream_contexts:
            # do not rerun if orange box version matches and upstream context matches
            logger.warning(
                "Sample %s has up-to-date analysis tables, skipping.",
                context.climb_id,
            )
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "are up-to-date - current upstream context: %s. "
                "Decision: not run.",
                context.climb_id,
                list(analysis_tables.keys()),
                current_context,
            )
            return False
        else:
            # This combination has not yet been run:
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Current "
                "upstream context is %s, which does not match any "
                "analysis tables %s. Decision: run.",
                context.climb_id,
                list(analysis_tables.keys()),
                current_context,
                upstream_contexts,
            )
            return True


class OrangeBoxWorker(Worker):
    def validate(self) -> None:
        """
        Overwrite method to check expected queues - priority and exchange.

        Allows backwards compatbility (without running rerun and priority), but
        will write a warning in the log.

        If either the exchange OR queue suffix is given but the other is not,
        this raises an error.

        Acts as safety net check rather than user friendly descriptive UI.

        Raises:
            WorkerError - if any required checks fail.
        """
        # Listen
        super().validate()

        # Publish
        if not self.publish_exchange or not self.publish_queue_suffix:
            raise WorkerError(
                "Orange box worker expects publish exchange and publish "
                "queue suffix set - check worker config."
            )

        # Priority
        if not self.priority_exchange and not self.priority_queue_suffix:
            logger.warning(
                "Orange Box Priority Message Queue not set, priority "
                "messages will NOT be consumed."
            )
        if bool(self.priority_exchange) != bool(self.priority_queue_suffix):
            raise WorkerError(
                "For priority queue consumption, both the priority exchange "
                "AND priority queue suffix must be set, check worker config. "
            )

        # Rerun
        if not self.rerun_exchange and not self.rerun_queue_suffix:
            logger.warning(
                "Orange Box Rerun Message Queue not set, rerun "
                "messages will NOT be consumed."
            )
        if bool(self.rerun_exchange) != bool(self.rerun_queue_suffix):
            raise WorkerError(
                "For rerun queue consumption, both the rerun exchange "
                "AND rerun queue suffix must be set, check worker config. "
            )

    def get_message(self) -> Any | None:
        """
        Overwrites the default Worker method to handle priority and rerun
        queues if provided in config.
        Orange Box has three queues to consume from. The listening queue is the
        main ingest queue, plus there is a low priority rerun queue for
        messages that are being rerun through the entire pipeline, and finally
        there is a high priority rerun queue local to orange box only, such
        that everything downstream of chimera can be rerun.

        Returns: one varys_client message object.
        """
        if self.priority_exchange:
            priority_message: Any = self._varys_client.receive(
                exchange=self.priority_exchange,
                queue_suffix=self.priority_queue_suffix,
                prefetch_count=1,
                timeout=1,
            )
        else:
            priority_message = None

        main_message: Any = self._varys_client.receive(
            exchange=self.listen_exchange,
            queue_suffix=self.listen_queue_suffix,
            prefetch_count=1,
            timeout=1,
        )
        if self.rerun_exchange:
            # low priority rerun queue
            rerun_message: Any = self._varys_client.receive(
                exchange=self.rerun_exchange,
                queue_suffix=self.rerun_queue_suffix,
                prefetch_count=1,
                timeout=1,
            )
        else:
            rerun_message = None

        # Handle the message that is returned, nack any other messages if
        # present
        if priority_message:
            message = priority_message
            logger.info("Consuming from priority queue.")
            if rerun_message:
                self._varys_client.nack_message(rerun_message)
            if main_message:
                self._varys_client.nack_message(main_message)
        elif main_message:
            message = main_message
            logger.info("Consuming from main queue.")
            if rerun_message:
                self._varys_client.nack_message(rerun_message)
        elif rerun_message:
            message = rerun_message
            logger.info("Consuming from rerun queue.")
        else:
            message = None
        return message

    def on_success(self, message: Any, context: PipelineContext) -> None:
        """Handle successful orange box pipeline completions.

        Add upstream context to payload, publish the new message with new
        payload to downstream queue then acknowledge the original message.

        Args:
            message: The Varys message object associated with the current
                sample.
            context: the object holding information about the current upstream
            context.

        Raises:
            Exception: If the Varys client fails to publish or acknowledge the
                message.
        """
        downstream_payload = context.payload.copy()
        # payload should store orange box version and onyx versions hash
        downstream_payload["onyx_versions_hash"] = context.onyx_versions_hash
        downstream_payload["orange_box_version"] = context.orange_box_version

        if self.publish_queue_suffix:
            self._varys_client.send(
                message=downstream_payload,
                exchange=self.publish_exchange,
                queue_suffix=self.publish_queue_suffix,
            )

        self._varys_client.acknowledge_message(message)

    def on_skip(self, message: Any, context: PipelineContext) -> None:
        """Handle messages that should be skipped.

        The same should happen as on success - add the current context to the
        payload, send new message to publish queue and ack message on listening
        queue.

        Args:
            message: The Varys message object associated with the current
            sample.
            context: the object holding information about the current upstream
            context.

        Raises:
            Exception: If the Varys client fails to acknowledge the message.
        """
        # skip mimics on_success for the orange box - on_skip still needs to
        # populate the payload in the message.
        self.on_success(message=message, context=context)


def build_worker(
    config: CheramiConfig,
    work_dir: Path,
    output_dir: Path,
    audit_db_path: Path,
) -> Worker:
    pipeline = build_pipeline(config.pipeline_config, config.global_config)
    return OrangeBoxWorker(
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

import datetime
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cherami.audit_db import AuditDB
from cherami.config import WorkerConfig, hash_from_file
from cherami.pipeline_runner import (
    NonRetryablePipelineError,
    PipelineRunner,
    RetryablePipelineError,
)
from cherami.pipelines import Pipeline
from cherami.pipelines.pipeline import PipelineContext
from cherami.utils import init_kubernetes, init_varys

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Result of a pipeline execution attempt."""

    climb_id: str
    job_uuid: str
    pipeline_name: str
    status: str
    error_message: str | None = None
    attempt: int | None = None
    max_attempts: int | None = None
    start_time: str | None = None
    end_time: str | None = None
    duration: float | None = None


class Worker:
    """Base worker for running pipelines.

    Consumes messages from a RabbitMQ queue via Varys and launches Nextflow
    pipelines using Kubernetes Jobs.

    This base class implements the core orchestration logic common to every
    worker. Subclasses can override event handlers (`on_skip`, `on_success`,
    `on_retry`, `on_sample_failure`) to define custom behavior.

    Attributes:
        config: Worker config object.
        pipeline: Pipeline object that the worker will run.
        _runner: Pipeline runner object.
        _varys_client: Varys client.
        _retry_counts: Map of retry attempts by climb ID.
        work_dir: Working directory for pipeline execution.
        output_dir: Output directory for pipeline results.
        _audit_db: Audit database object logging pipeline events.
        listen_exchange: Varys exchange name for incoming jobs.
        listen_queue_suffix: Queue suffix for incoming jobs used by varys for
            queue names.
        varys_config_path: Path to the Varys configuration file.
        varys_log_path: Path to the Varys log file.
        publish_queue_suffix: Optional queue suffix for completion messages.
        publish_exchange: Optional exchange for completion messages.
        _config_path: Path to the worker configuration file.
        _startup_config_hash: Hash of the configuration at startup.
    """

    def __init__(
        self,
        worker_config: WorkerConfig,
        pipeline: Pipeline,
        work_dir: Path,
        output_dir: Path,
        audit_db_path: Path,
    ) -> None:
        self.config: WorkerConfig = worker_config
        self.pipeline: Pipeline = pipeline
        self._runner: PipelineRunner
        self._varys_client: Any
        self._retry_counts: dict[str, int] = {}
        self.work_dir: Path = work_dir
        self.output_dir: Path = output_dir
        self._audit_db: AuditDB = AuditDB(audit_db_path)
        self.listen_exchange: str = worker_config.listen_exchange
        self.listen_queue_suffix: str = worker_config.listen_queue_suffix
        self.varys_config_path: Path = worker_config.varys_config_path
        self.varys_log_path: Path = worker_config.varys_log_path
        self.publish_queue_suffix: str | None = (
            worker_config.publish_queue_suffix
        )
        self.publish_exchange: str | None = worker_config.publish_exchange
        self._config_path: Path = worker_config.config_path
        self._startup_config_hash: str = worker_config.config_hash

    def on_skip(self, message: Any, context: PipelineContext) -> None:
        """Handle messages that should be skipped.

        The default implementation acknowledges the message to remove it from
        the queue.

        Override this method to implement custom logic for skipped samples.

        Args:
            message: The Varys message object associated with the current
            sample.
            context: the object holding information about the current upstream
            context.

        Raises:
            Exception: If the Varys client fails to acknowledge the message.
        """
        self._varys_client.acknowledge_message(message)

    def on_success(self, message: Any, context: PipelineContext) -> None:
        """Handle successful pipeline completions.

        Publishes the result to a downstream queue if `publish_queue_suffix` is
        configured, then acknowledges the original message.
        This enables chaining workers where one worker's output queue becomes
        the next worker's input.

        Override this method to implement custom post-processing logic.

        Args:
            message: The Varys message object associated with the current
                sample.
            context: the object holding information about the current upstream
            context.

        Raises:
            Exception: If the Varys client fails to publish or acknowledge the
                message.
        """
        ## if a worker configured a publish queue, this sends that message to
        ## the publish_exchange
        if self.publish_queue_suffix:
            self._varys_client.send(
                message=context.payload,
                exchange=self.publish_exchange,
                queue_suffix=self.publish_queue_suffix,
            )

        self._varys_client.acknowledge_message(message)

    def on_retry(
        self,
        message: Any,
    ) -> None:
        """Handle pipeline failures eligible for retry.

        The default implementation negatively acknowledges (nacks) the message,
        returning it to the queue for redelivery. The worker tracks retry
        counts internally and calls to `on_sample_failure` if `max_attempts` is
        exhausted.

        Override this method to implement custom retry strategies.

        Args:
            message: The Varys message object associated with the current
                sample.

        Raises:
            Exception: If the Varys client fails to requeue the message.
        """
        self._varys_client.nack_message(message)

    def on_sample_failure(
        self,
        message: Any,
    ) -> None:
        """Handle permanent pipeline failures.

        Invoked when a sample fails and is not eligible for retry (or has
        exhausted all retry attempts). This method has no default
        implementation.

        Override this method to handle terminal failures, such as sending the
        message to a dead-letter queue, logging a detailed error report, or
        alerting an administrator.

        Args:
            message: The Varys message object associated with the current
                sample.
        """
        ## TODO: consider publishing to an error queue if configured

    def _parse_message(
        self,
        message: Any,
    ) -> tuple[dict[str, Any], str, str]:
        """Extract sample information from the message.

        Returns the message payload, sample ID (climb_id), and job UUID
        (match_uuid).

        Args:
            message: The Varys message object associated with the current
            sample.

        Returns:
            A tuple containing:
            - The full message payload dictionary.
            - The sample ID (climb_id).
            - The job UUID (match_uuid).

        Raises:
            ValueError: If the message body is invalid JSON or missing required
            fields.
        """

        try:
            payload = json.loads(message.body)
        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON in varys message") from e

        climb_id = payload.get("climb_id")
        job_uuid = payload.get("match_uuid")

        if not climb_id or not job_uuid:
            raise ValueError("Message missing climb_id or match_uuid")

        return payload, climb_id, job_uuid

    def _create_result(
        self,
        climb_id: str,
        job_uuid: str,
        status: str,
        error_message: str | None = None,
        attempt: int | None = None,
        max_attempts: int | None = None,
        start_time: datetime.datetime | None = None,
        end_time: datetime.datetime | None = None,
    ) -> PipelineResult:
        """Create a structured result object for audit logging.

        Returns a PipelineResult suitable for audit logging. Duration is
        populated only when both start and end timestamps are provided.

        Args:
            climb_id: Sample identifier.
            job_uuid: Unique job UUID.
            status: Outcome of the pipeline execution
                (SUCCESS, FAILED, SKIPPED, RETRY).
            error_message: Description of the error if failed or retried.
            attempt: Current attempt number.
            max_attempts: Total allowed attempts.
            start_time: Timestamp when execution started.
            end_time: Timestamp when execution finished.

        Returns:
            A PipelineResult used for the audit database.
        """
        duration = (
            (end_time - start_time).total_seconds()
            ## skipped samples dont have start/end times
            if start_time is not None and end_time is not None
            else None
        )

        start_time_str = start_time.isoformat("T") if start_time else None
        end_time_str = end_time.isoformat("T") if end_time else None

        return PipelineResult(
            climb_id=climb_id,
            job_uuid=job_uuid,
            pipeline_name=self.pipeline.config.name,
            status=status,
            error_message=error_message,
            attempt=attempt,
            max_attempts=max_attempts,
            start_time=start_time_str,
            end_time=end_time_str,
            duration=duration,
        )

    def run(self) -> None:
        """Execute the main worker loop.

        Runs the worker until it exits.

        Raises:
            RuntimeError: If the worker exits due to a pipeline error or client
                initialisation failure.
            ValueError: If an incoming message cannot be parsed.
            Exception: If an unexpected error occurs and the worker exits.
        """
        logger.info("Serving worker: %s", self.pipeline.config.name)
        self._varys_client = init_varys(
            self.varys_config_path,
            self.varys_log_path,
            "cherami",
        )
        logger.info("Worker listening on exchange %s", self.listen_exchange)
        self._runner = PipelineRunner(
            k8_api=init_kubernetes(),
        )
        audit_db = self._audit_db
        pipeline = self.pipeline
        message = None
        try:
            while True:
                ## the basic flow  of a worker is first check first for any messages - listening to varys is blocking.
                ## If there are no messages after the timeout, poll again and wait. If there is a message, parse it to
                ## get sample_id and uuid and call the `should_run` method on the pipeline to see if passess decision
                ## logic. If it does not pass, call `on_skip` to ack and move to next sample. If it does pass, call
                ## `run_pipeline` on the `PipelineRunner` instance to then launch the pipeline. Exceptions indicate
                ## failure states. If success, call `on_success` to ack and potentially publish to next queue. If
                ## failure, it will be retried up to `max_attempts`, calling `on_retry` to nack the message so
                ## it goes back to the queue. If max_attempts is exhausted, call `on_sample_failure` to ack and handle
                # the error to move on.
                try:
                    message = self._varys_client.receive(
                        exchange=self.listen_exchange,
                        queue_suffix=self.listen_queue_suffix,
                        prefetch_count=1,
                        timeout=1,
                    )
                    if not message:
                        time.sleep(5)
                        continue

                    payload, climb_id, job_uuid = self._parse_message(message)
                    logger.info(
                        "Received message climb id: %s uuid: %s",
                        climb_id,
                        job_uuid,
                    )

                    # Once we have the message, get the upstream onyx context:
                    upstream_context: PipelineContext = pipeline.build_context(
                        payload=payload
                    )

                    if not pipeline.should_run(upstream_context):
                        logger.info(
                            "Criteria not met for sample %s; acknowledging "
                            "message.",
                            climb_id,
                        )
                        result: PipelineResult = self._create_result(
                            climb_id=climb_id,
                            job_uuid=job_uuid,
                            status="SKIPPED",
                        )
                        audit_db.add_record(result)
                        self.on_skip(message, upstream_context)
                        continue

                    current_config_hash = hash_from_file(self._config_path)
                    if current_config_hash != self._startup_config_hash:
                        logger.warning(
                            "Config file has changed since startup. "
                            "Please restart the worker to apply changes.",
                        )

                    total_attempts = pipeline.config.max_attempts
                    current_attempt = self._retry_counts.get(climb_id, 0) + 1
                    self._retry_counts[climb_id] = current_attempt

                    logger.info(
                        "Worker running sample %s (attempt %d/%d)",
                        climb_id,
                        current_attempt,
                        total_attempts,
                    )
                    start_time: datetime.datetime = datetime.datetime.now(
                        datetime.UTC
                    )

                    try:
                        self._runner.run_pipeline(
                            pipeline=pipeline,
                            sample_id=climb_id,
                            job_uuid=job_uuid,
                            worker_work_dir=self.work_dir,
                            worker_output_dir=self.output_dir,
                            execution_timestamp=start_time,
                            context=upstream_context,
                        )
                    except RetryablePipelineError as e:
                        end_time = datetime.datetime.now(datetime.UTC)
                        error_message = str(e)
                        if current_attempt >= total_attempts:
                            self._retry_counts.pop(climb_id, None)
                            result = self._create_result(
                                climb_id=climb_id,
                                job_uuid=job_uuid,
                                status="FAILED",
                                error_message=error_message,
                                attempt=current_attempt,
                                max_attempts=total_attempts,
                                start_time=start_time,
                                end_time=end_time,
                            )
                            audit_db.add_record(result)
                            logger.error(
                                "Pipeline retries exhausted for sample %s job "
                                "%s pipeline %s (attempt %d/%d): %s",
                                climb_id,
                                job_uuid,
                                pipeline.config.name,
                                current_attempt,
                                total_attempts,
                                error_message,
                            )
                            raise RuntimeError(
                                "Pipeline retries exhausted"
                            ) from e

                        next_attempt = current_attempt + 1
                        logger.warning(
                            "Retrying pipeline %s for sample %s job %s "
                            "(next attempt %d/%d): %s",
                            pipeline.config.name,
                            climb_id,
                            job_uuid,
                            next_attempt,
                            total_attempts,
                            error_message,
                        )
                        result = self._create_result(
                            climb_id=climb_id,
                            job_uuid=job_uuid,
                            status="RETRY",
                            error_message=error_message,
                            attempt=current_attempt,
                            max_attempts=total_attempts,
                            start_time=start_time,
                            end_time=end_time,
                        )
                        audit_db.add_record(result)
                        self.on_retry(message)
                        continue
                    except NonRetryablePipelineError as e:
                        end_time = datetime.datetime.now(datetime.UTC)
                        self._retry_counts.pop(climb_id, None)
                        result = self._create_result(
                            climb_id=climb_id,
                            job_uuid=job_uuid,
                            status="FAILED",
                            error_message=str(e),
                            attempt=current_attempt,
                            max_attempts=total_attempts,
                            start_time=start_time,
                            end_time=end_time,
                        )
                        audit_db.add_record(result)
                        logger.error(
                            "Non-retryable pipeline error for sample %s job "
                            "%s pipeline %s (attempt %d/%d): %s",
                            climb_id,
                            job_uuid,
                            pipeline.config.name,
                            current_attempt,
                            total_attempts,
                            str(e),
                        )
                        raise RuntimeError(
                            "Non-retryable pipeline error"
                        ) from e

                    end_time = datetime.datetime.now(datetime.UTC)
                    self._retry_counts.pop(climb_id, None)
                    result = self._create_result(
                        climb_id=climb_id,
                        job_uuid=job_uuid,
                        status="SUCCESS",
                        attempt=current_attempt,
                        max_attempts=total_attempts,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    audit_db.add_record(result)
                    ## TODO: decide when to actually mark as success - if something fails after the pipeline run but before here,
                    ## then the sample will be retried even though the pipeline itself succeeded and possibly duplicate analysis tables etc
                    ## can we have a check we can add to should_run to see if a characterisation pipeline has already run for this sample
                    self.on_success(message, upstream_context)
                except RuntimeError:
                    logger.error("Worker stopping due to pipeline failure")
                    raise
                except Exception as e:
                    logger.exception(
                        "Unhandled exception in worker: %s", str(e)
                    )
                    raise
        finally:
            logger.info("%s worker stopping", self.pipeline.config.name)
            self._varys_client.close()

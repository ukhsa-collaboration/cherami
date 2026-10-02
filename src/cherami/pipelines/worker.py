import datetime
import json
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cherami.audit_db import AuditDB
from cherami.config import WorkerConfig, hash_from_file
from cherami.exceptions import (
    CheramiError,
    ConfigurationError,
    NonRetryablePipelineError,
    RetryableError,
    RetryablePipelineError,
    SampleError,
)
from cherami.pipeline_runner import PipelineRunner
from cherami.pipelines import Pipeline
from cherami.pipelines.pipeline import PipelineContext
from cherami.utils import (
    init_kubernetes,
    init_varys,
)

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
        rerun_queue_suffix: Optional queue suffix for upstream rerun.
        rerun_exchange: Optional exchange for upstream rerun (NOTE: this is
            not used to push messages to).
        priority_queue_suffix: Optional queue suffix for priority queue.
        priority_exchange: Optional exchange name for the priority messages.
        dead_sample_queue_suffix: queue for failing samples to be routed to.
        dead_sample_exchange: Exchange to send failing samples to.
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
        self.rerun_exchange: str | None = worker_config.rerun_exchange
        self.rerun_queue_suffix: str | None = worker_config.rerun_queue_suffix
        self.priority_exchange: str | None = worker_config.priority_exchange
        self.priority_queue_suffix: str | None = (
            worker_config.priority_queue_suffix
        )
        self.dead_sample_exchange: str | None = (
            worker_config.dead_sample_exchange
        )
        self.dead_sample_queue_suffix: str | None = (
            worker_config.dead_sample_queue_suffix
        )
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
        context: PipelineContext,
        reason: str,
        error: RetryablePipelineError | NonRetryablePipelineError | Exception,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        current_attempt: int,
        max_attempts: int,
    ) -> None:
        """Handle permanent pipeline failures for samples by either sending
        message to dead sample queue if configured OR raising errors.

        Invoked when a sample fails and is not eligible for retry (or has
        exhausted all retry attempts). IF the dead sample exchange is
        configured, a new message is published to the 'dead sample exchange',
        which will route the message to the pipeline specific queue using the
        routing key (set in the varys_client object).

        Args:
            message: The Varys message object associated with the current
                sample.
            context: PipelineContext object.
            reason: Reason for failure.
            error: the error message captured from the exception
            start_time: start time of sample handling
            end_time: end time of sampling handling
            current_attempt: how many times has sample handling been attempted.
            max_attempts: how many times sample handling can be attempted.

        """
        new_payload = context.payload
        new_payload["start_time"] = start_time
        new_payload["end_time"] = end_time
        new_payload["pipeline_name"] = self.pipeline.config.name
        new_payload["failure_type"] = reason
        new_payload["error_message"] = str(error)
        new_payload["max_attempts"] = max_attempts
        new_payload["attempts"] = current_attempt

        logger.warning(
            "Pipeline cannot be retried for sample %s. Sending to dead "
            "sample queue %s",
            context.climb_id,
            self.dead_sample_queue_suffix,
        )

        self._varys_client.send(
            message=context.payload,
            exchange=self.dead_sample_exchange,
            queue_suffix=self.dead_sample_queue_suffix,
            exchange_type="direct",
            max_attempts=3,
        )
        # Only acknowledge if dead sample queue defined
        self._varys_client.acknowledge_message(message)

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
            SampleError: If the message body is invalid JSON, missing
            required fields or the climb ID is not in the correct format.
        """

        try:
            payload = json.loads(message.body)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in varys message")
            raise SampleError("invalid_message") from e

        climb_id = payload.get("climb_id")
        job_uuid = payload.get("match_uuid")

        # check climb_id integrity
        if not bool(re.fullmatch("C-[A-F0-9]{10}", climb_id)):
            logger.error(
                "Climb ID %s in message (uuid: %s) is malformed. "
                "Cannot continue.",
                climb_id,
                job_uuid,
            )
            raise SampleError("malformed_id_in_message")

        # check missing info:
        if not climb_id or not job_uuid:
            raise SampleError(
                "Message missing climb_id or match_uuid: %s.", message
            )

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

    def validate(self) -> None:
        """
        Pre-flight checks for the worker config.

        Listening exchange AND queue must be set.

        Raises:
            ConfigurationError - if any checks fail, enter sleep state - do not
            consume messages.
        """
        # Listen exchange and queue cannot be None
        if not self.listen_exchange or not self.listen_queue_suffix:
            logger.error(
                "Listen exchange and/or queue suffix has not been set, "
                "cannot consume messages."
            )
            raise ConfigurationError("listen_exchange_config")

        # check if dead-sample exchange set, that is is named suitably
        if not self.dead_sample_exchange or not self.dead_sample_queue_suffix:
            logger.error(
                "Dead Sample Exchange and dead sample queue suffix have not "
                "been set, cannot continue."
            )
            raise ConfigurationError("dead_sample_exchange_config")

    def get_message(self) -> Any | None:
        """
        Default message consumption is to listen to one queue.

        Overwrite this message to implement more complex message consumption
        or priority.

        Returns: varys_client message object or None.

        """
        message: Any = self._varys_client.receive(
            exchange=self.listen_exchange,
            queue_suffix=self.listen_queue_suffix,
            prefetch_count=1,
            timeout=1,
        )
        return message

    def run(self) -> None:
        """Execute the main worker loop.

        Runs the worker until it exits.

        Raises:
            ConfigurationError: if any of the configs are not set up correctly.
            RetryableError: any errors that could be retried such as Onyx
                queries.
            SampleError: if there is an issue with a sample; either in the
                pipeline or the message itself.
            RuntimeError: If the worker exits due to a client initialisation
            failure.
            ValueError: If an incoming message cannot be parsed.
            Exception: If an unexpected error occurs and the worker exits.

        """
        # Start up
        logger.info("Serving worker: %s", self.pipeline.config.name)

        logger.info("Validating...")
        self.validate()

        # init varys
        self._varys_client = init_varys(
            self.varys_config_path,
            self.varys_log_path,
            "cherami",
            routing_key=self.pipeline.config.name
            if self.dead_sample_exchange
            else "arbitary_string",  # this is Varys default
        )
        logger.info(
            "Worker listening on main exchange %s queue %s ",
            self.listen_exchange,
            self.listen_queue_suffix,
        )
        if self.rerun_exchange:
            logger.info(
                "Worker listening on rerun exchange %s queue %s ",
                self.rerun_exchange,
                self.rerun_queue_suffix,
            )
        if self.priority_exchange:
            logger.info(
                "Worker listening on priority exchange %s queue %s ",
                self.priority_exchange,
                self.priority_queue_suffix,
            )
        self._runner = PipelineRunner(
            k8_api=init_kubernetes(),
        )
        audit_db = self._audit_db
        pipeline = self.pipeline
        message = None

        # The basic flow of a worker is first check first for any messages -
        # listening to varys is blocking. If there are no messages after the
        # timeout, poll again and wait. If there is a message, parse it to get
        # sample_id and uuid and call the `should_run` method on the pipeline
        # to see if passess decision logic.
        # If it does not pass, call `on_skip`.
        # If it does pass, call `run_pipeline` on the `PipelineRunner` instance
        # to then launch the pipeline. Exceptions indicate failure states.
        # If pipeline runner is a success, call `on_success` to ack and
        # potentially publish to next queue (if configured as such).
        # If failure, it will be retried up to `max_attempts`, calling
        # `on_retry` to nack the message so it goes back to the queue.
        # If max_attempts is exhausted, call `on_sample_failure` to send to
        # dead sample exchange (if configured) and ack, else raise error.

        # NOTES - pull apart worker retry and pipeline retry. Currently onyx time outs will block a
        # message infinitely.

        try:
            while True:
                try:  # exceptions for Runtime error or generic Exception
                    message = self.get_message()

                    # Poll for a message:
                    if not message:
                        time.sleep(10)
                        continue

                    # Got a message! Parse it:
                    payload, climb_id, job_uuid = self._parse_message(message)
                    logger.info(
                        "Received message climb id: %s uuid: %s",
                        climb_id,
                        job_uuid,
                    )

                    # get the upstream onyx context for this sample:
                    try:
                        upstream_context: PipelineContext = (
                            pipeline.build_context(payload=payload)
                        )
                    except ValueError as v:
                        raise SampleError from v

                    # Decision time - run the pipeline?
                    should_run = pipeline.should_run(upstream_context)

                    if not should_run:
                        # Skipping
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

                    # Run the Pipeline
                    # Setup
                    current_config_hash = hash_from_file(self._config_path)
                    if current_config_hash != self._startup_config_hash:
                        logger.warning(
                            "Pipeline config file has changed since startup. "
                            "Please restart the worker to apply changes.",
                        )
                        raise ConfigurationError("pipeline_config_json")

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

                    # Run it
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

                    # It failed but can retry:
                    except RetryablePipelineError as e:
                        end_time = datetime.datetime.now(datetime.UTC)
                        error_message = str(e)

                        # Exceeding retry limits:
                        if current_attempt >= total_attempts:
                            # Attempts have been exhausted
                            self._retry_counts.pop(climb_id, None)
                            # Create the result to be logged in the audit db
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
                            # Write error to log
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
                            # handle sample failure - send to DLQ or raise
                            self.on_sample_failure(
                                message=message,
                                context=upstream_context,
                                reason="retries_exhaused",
                                error=e,
                                start_time=start_time,
                                end_time=end_time,
                                current_attempt=current_attempt,
                                max_attempts=total_attempts,
                            )

                        # Attempt again
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
                        result: PipelineResult = self._create_result(
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

                    # Failed in non-retryable way
                    except NonRetryablePipelineError as e:
                        end_time = datetime.datetime.now(datetime.UTC)
                        self._retry_counts.pop(climb_id, None)
                        result: PipelineResult = self._create_result(
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

                        # handle sample failure - send to DLQ or raise

                        self.on_sample_failure(
                            message=message,
                            context=upstream_context,
                            reason="non-retryable pipeline",
                            error=e,
                            start_time=start_time,
                            end_time=end_time,
                            current_attempt=current_attempt,
                            max_attempts=total_attempts,
                        )
                        continue

                    # Pipeline completed successfully
                    end_time = datetime.datetime.now(datetime.UTC)
                    self._retry_counts.pop(climb_id, None)
                    result: PipelineResult = self._create_result(
                        climb_id=climb_id,
                        job_uuid=job_uuid,
                        status="SUCCESS",
                        attempt=current_attempt,
                        max_attempts=total_attempts,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    audit_db.add_record(result)

                    self.on_success(message, upstream_context)

                # If the message failed at the worker level due to connection
                # error and should be retried. This is a 'stay alive' loop.
                except RetryableError as r:
                    # Write error to log
                    logger.error(
                        "Sample %s could not be processed - %s, retrying "
                        "in 1hr.",
                        climb_id,
                        str(r),
                    )

                    # nack message
                    self.on_retry(message)
                    time.sleep(10)
                    continue

                # If the message cannot be processed, it is sent to the dead
                # sample queue:
                except SampleError as nr:
                    self.on_sample_failure(
                        message=message,
                        context=upstream_context,
                        reason="non-retryable worker",
                        error=nr,
                        start_time=start_time,
                        end_time=end_time,
                        current_attempt=current_attempt,
                        max_attempts=total_attempts,
                    )
                    continue

                # If there is a config or set up error, stop the worker
                except CheramiError as w:
                    # nack message
                    self.on_retry(message)
                    logger.error(
                        "Worker cannot continue to process sample: %s.", w
                    )
                    time.sleep(10)
                    raise
                except Exception as e:
                    logger.exception(
                        "Unhandled exception in worker: %s", str(e)
                    )
                    time.sleep(10)
                    raise

        finally:
            # We hit this when the worker stops
            logger.info("%s worker stopping", self.pipeline.config.name)
            self._varys_client.close()

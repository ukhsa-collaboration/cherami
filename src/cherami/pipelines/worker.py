import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cherami.audit_db import AuditDB
from cherami.config import WorkerConfig
from cherami.pipeline_runner import (
    NonRetryablePipelineError,
    PipelineRunner,
    RetryablePipelineError,
)
from cherami.pipelines import Pipeline
from cherami.utils import init_kubernetes, init_varys


@dataclass
class PipelineResult:
    """Result of a pipeline execution attempt, created by Worker for audit logging."""

    climb_id: str
    job_uuid: str
    pipeline_name: str
    status: str
    error_message: str | None = None
    attempt: int | None = None
    max_attempts: int | None = None
    start_time: float | None = None
    end_time: float | None = None
    duration: float | None = None


class Worker:
    """Defines a base template for all workers.

    Consumes messages from a RabbitMQ queue via Varys and launches Nextflow
    pipelines using Kubernetes Jobs.

    This base class implements the core orchestration logic common to every
    worker. Subclasses can override event handlers (`on_skip`, `on_success`,
    `on_retry`, `on_sample_failure`) to define custom behavior.

    Attributes:
        worker_name: Human-readable identifier for the worker.
        listen_exchange: Varys exchange name for incoming jobs.
        listen_queue_suffix: Specific queue suffix for incoming jobs.
        varys_config_path: Path to the Varys configuration file.
        varys_log_path: Path to the Varys log file.
        publish_queue_suffix: Optional queue suffix for completion messages.
        publish_exchange: Optional exchange for completion messages.
    """

    def __init__(
        self,
        worker_config: WorkerConfig,
        pipeline: Pipeline,
        work_dir: Path,
        output_dir: Path,
        audit_db_path: Path | None = None,
    ) -> None:
        self.config = worker_config
        self.pipeline = pipeline
        self.worker_name: str = worker_config.worker_name
        self.listen_exchange: str = worker_config.listen_exchange
        self.listen_queue_suffix: str = worker_config.listen_queue_suffix
        self.varys_config_path: Path = worker_config.varys_config_path
        self.varys_log_path: Path = worker_config.varys_log_path
        self.publish_queue_suffix: str | None = (
            worker_config.publish_queue_suffix
        )
        self.publish_exchange: str | None = worker_config.publish_exchange
        self._varys_client: Any
        self._runner: PipelineRunner
        self.logger = logging.getLogger(
            f"cherami.pipelines.{worker_config.worker_name}"
        )
        self._retry_counts: dict[str, int] = {}
        self.work_dir = work_dir
        self.output_dir = output_dir
        self._audit_db = AuditDB(audit_db_path) if audit_db_path else None

    def on_skip(self, message: Any) -> None:
        """Handle messages that should be skipped.

        The default implementation acknowledges the message to remove it from the
        queue.

        Override this method to implement custom logging or side effects for
        skipped samples, such as writing to a specific "skipped" log or queue.

        Args:
            message: The Varys message object associated with the current sample.
        """
        self._varys_client.acknowledge_message(message)

    def on_success(self, message: Any, payload: dict[str, Any]) -> None:
        """Handle successful pipeline completions.

        Publishes the result to a downstream queue if `publish_queue_suffix` is
        configured, then acknowledges the original message. If `publish_exchange`
        is not set, it defaults to publishing to the worker's `listen_exchange`.
        This enables chaining workers where one worker's output queue becomes
        the next worker's input.

        Override this method to implement custom post-processing logic, such as
        sending notifications, updating external databases, or modifying the
        payload before downstream publication.

        Args:
            message: The Varys message object associated with the current sample.
            payload: The message payload to publish downstream.
        """
        ## if a worker configured a publish queue, this  send that message to the listen_exchange,
        ## unless the worker ALSO configures a publish_exchange, in which case use that
        if self.publish_queue_suffix:
            target_exchange = self.publish_exchange or self.listen_exchange
            self._varys_client.send(
                message=payload,
                exchange=target_exchange,
                queue_suffix=self.publish_queue_suffix,
            )

        self._varys_client.acknowledge_message(message)

    def on_retry(
        self,
        message: Any,
    ) -> None:
        """Handle pipeline failures eligible for retry.

        The default implementation negatively acknowledges (nacks) the message,
        returning it to the queue for redelivery. The worker tracks retry counts
        internally and escalates to `on_sample_failure` when `max_retries` is
        exceeded.

        Override this method to implement custom retry strategies, such as
        implementing exponential backoff (if supported by the queue) or logging
        retry attempts to a monitoring service.

        Args:
            message: The Varys message object associated with the current sample.
        """
        self._varys_client.nack_message(message)

    def on_sample_failure(
        self,
        message: Any,
    ) -> None:
        """Handle permanent pipeline failures.

        Invoked when a sample fails and is not eligible for retry (or has
        exhausted all retry attempts). This method has no default implementation.

        Override this method to handle terminal failures, such as sending the
        message to a dead-letter queue, logging a detailed error report, or
        alerting an administrator.

        Args:
            message: The Varys message object associated with the current sample.
        """
        ## TODO: consider publishing to an error queue if configured

    def _parse_message(
        self,
        message: Any,
    ) -> tuple[dict[str, Any], str, str]:
        """Extract sample information from the message.

        Parses the JSON body of the message to retrieve the payload, sample ID
        (climb_id), and job UUID.

        Args:
            message: The Varys message object associated with the current sample.

        Returns:
            A tuple containing:
            - The full message payload dictionary.
            - The sample ID (climb_id).
            - The job UUID (match_uuid).

        Raises:
            ValueError: If the message body is invalid JSON or missing required fields.
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
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> PipelineResult:
        """Create a structured result object for audit logging.

        Helper method to instantiate `PipelineResult`, calculating the duration
        automatically if start and end times are provided.

        Args:
            climb_id: Sample identifier.
            job_uuid: Unique job UUID.
            status: Outcome of the pipeline execution (SUCCESS, FAILED, SKIPPED, RETRY).
            error_message: Description of the error if failed or retried.
            attempt: Current attempt number.
            max_attempts: Total allowed retry attempts.
            start_time: Timestamp when execution started.
            end_time: Timestamp when execution finished.

        Returns:
            A PipelineResult used for the audit database.
        """
        duration = (
            end_time - start_time
            ## skipped samples dont have start/end times
            if start_time is not None and end_time is not None
            else None
        )
        return PipelineResult(
            climb_id=climb_id,
            job_uuid=job_uuid,
            pipeline_name=self.pipeline.config.name,
            status=status,
            error_message=error_message,
            attempt=attempt,
            max_attempts=max_attempts,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
        )

    def run(self) -> None:
        """Execute the main worker loop.

        Connects to Varys and the Kubernetes API, then enters a continuous loop
        to process incoming messages. It handles the full lifecycle of a sample:
        reception, validation, execution, and completion handling (success,
        retry, or failure).

        Exceptions during processing are logged and re-raised, causing the
        worker to exit.
        """
        self.logger.info("Serving worker: %s", self.worker_name)
        self._varys_client = init_varys(
            self.varys_config_path, self.varys_log_path
        )
        self.logger.info(
            "Worker listening on exchange %s", self.listen_exchange
        )
        self._runner = PipelineRunner(
            k8_api=init_kubernetes(),
        )
        audit_db = self._audit_db
        ## possibly happens if audit db env var is set as empty string
        if audit_db is None:
            raise RuntimeError(
                "Audit database path is required to run a worker"
            )
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
                ## failure, it will be retried up to `max_retries`, calling `on_retry` to nack the message so
                ## it goes back to the queue. If max_retries is exceeded, call `on_sample_failure` to ack and handle
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
                    self.logger.info(
                        "Received message climb id: %s uuid: %s",
                        climb_id,
                        job_uuid,
                    )

                    if not pipeline.should_run(climb_id):
                        self.logger.info(
                            "Criteria not met for sample %s; acknowledging message",
                            climb_id,
                        )
                        result = self._create_result(
                            climb_id=climb_id,
                            job_uuid=job_uuid,
                            status="SKIPPED",
                        )
                        audit_db.add_record(result)
                        self.on_skip(message)
                        continue

                    max_retries = pipeline.config.max_retries
                    total_attempts = max_retries + 1
                    current_attempt = self._retry_counts.get(climb_id, 0) + 1
                    self._retry_counts[climb_id] = current_attempt

                    self.logger.info(
                        "Worker running sample %s (attempt %d/%d)",
                        climb_id,
                        current_attempt,
                        total_attempts,
                    )
                    start_time = time.time()
                    try:
                        self._runner.run_pipeline(
                            pipeline=pipeline,
                            sample_id=climb_id,
                            job_uuid=job_uuid,
                            worker_work_dir=self.work_dir,
                            worker_output_dir=self.output_dir,
                        )
                    except RetryablePipelineError as e:
                        end_time = time.time()
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
                            self.logger.error(
                                "Pipeline retries exhausted for sample %s job %s pipeline %s (attempt %d/%d): %s",
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
                        self.logger.warning(
                            "Retrying pipeline %s for sample %s job %s (next attempt %d/%d): %s",
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
                        end_time = time.time()
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
                        self.logger.error(
                            "Non-retryable pipeline error for sample %s job %s pipeline %s (attempt %d/%d): %s",
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

                    end_time = time.time()
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
                    self.on_success(message, payload)
                except RuntimeError:
                    self.logger.error(
                        "Worker stopping due to pipeline failure"
                    )
                    raise
                except Exception as e:
                    self.logger.exception(
                        "Unhandled exception in worker: %s", str(e)
                    )
                    raise
        finally:
            self.logger.info("%s worker stopping", self.worker_name)
            self._varys_client.close()

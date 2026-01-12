import json
import logging
import time
from pathlib import Path
from typing import Any

from cherami.config import WorkerConfig
from cherami.pipeline_runner import (
    NonRetryablePipelineError,
    PipelineRunner,
    RetryablePipelineError,
)
from cherami.pipelines import Pipeline
from cherami.utils import init_kubernetes, init_varys


class Worker:
    """Defines a base template for all workers, consuming a RabbitMQ queue and launching
    pipelines.

    The base class implements orchestration methods common to every worker. It
    binds to the configured exchange and executes pipelines via the
    `PipelineRunner` module. Subclasses can optionally override `on_skip`,
    `on_success`, `on_retry`, and `on_sample_failure` to define additional
    behaviour when these events occur.

    For example, a subclass could override `on_success` to push a payload to another message queue, or
    override `on_sample_failure` to send an alert to an error message queue.

    Attributes:
        worker_name: Human-readable identifier for the worker
        listen_exchange: Name of the Varys exchange to listen to for incoming jobs.
        listen_queue_suffix: Specific queue suffix for the workers job.
        varys_config_path: File path for the Varys configuration file.
        varys_log_path: File path for the Varys log file.
        publish_queue_suffix: Optional suffix for the queue to publish completion messages to.
        publish_exchange: Optional exchange for completion messages.
    """

    def __init__(
        self,
        worker_config: WorkerConfig,
        pipeline: Pipeline,
        work_dir: Path,
        output_dir: Path,
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

    def on_skip(self, message: Any) -> None:
        """Called when a message is to be skipped.

        Called when `pipeline.should_run(sample_id)` returns `False`, indicating
        the sample should not be processed. The default implementation
        acknowledges the message and moves on. Override this if you need
        custom behavior when skipping samples, such as logging to a separate
        queue.
        """
        self._varys_client.acknowledge_message(message)

    def on_success(self, message: Any, payload: dict[str, Any]) -> None:
        """Called when a pipeline run completes successfully.

        The default implementation publishes the message to `publish_exchange`/`publish_queue_suffix`
        (if configured) and then acknowledges the original message. Publishing to another queue allows
        chaining workers together: one worker's `publish_queue_suffix` becomes the next worker's
        `listen_queue_suffix`.

        Override this if you need additional logic on success, e.g. triggering downstream notifications etc

        Args:
            message: Varys message representing a sample that completed successfully.
            payload: Parsed message payload used when republishing downstream.
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
        """Called when a pipeline run fails but is eligible for retry.

        The default implementation negatively acknowledges (nacks) the message, which returns it to the
        queue for another attempt. RabbitMQ will redeliver the message, and the worker will process it
        again later.

        The worker increments the retry count in `_retry_counts` each time a sample is attempted. Once
        the count reaches `pipeline.config.max_retries`, the worker calls `on_sample_failure` instead.

        Override this if you need custom retry behavior, such as changing retry behaviour or routing
        retries to a different queue.

        Args:
            message: Varys message representing a sample that should be retried.
        """
        self._varys_client.nack_message(message)

    def on_sample_failure(
        self,
        message: Any,
    ) -> None:
        """Called when a pipeline run fails and is NOT eligible for retry.

        The default implementation is a no-op. Override this if you need to handle failures
        specially, such as logging to an error queue, sending alerts, or writing to a dead
        letter queue.

        Args:
            message: Varys message representing a sample that has failed.
        """
        ## TODO: consider publishing to an error queue if configured

    def _parse_message(
        self,
        message: Any,
    ) -> tuple[dict[str, Any], str, str]:
        payload = json.loads(message.body)

        climb_id = payload.get("climb_id")
        job_uuid = payload.get("match_uuid")

        if not climb_id or not job_uuid:
            raise ValueError("Message missing climb_id or match_uuid")

        return payload, climb_id, job_uuid

    def run(self) -> None:
        """Main worker loop, consuming messages and launching pipelines as required.

        Main entry point for the worker. This is a long-running method that
        listens for messages on the configured Varys exchange/queue and launches
        pipelines as required.

        The loop only exits if the worker raises or the process is terminated.
        """
        self._varys_client = init_varys(
            self.varys_config_path, self.varys_log_path
        )
        self._runner = PipelineRunner(
            k8_api=init_kubernetes(),
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

                    if not pipeline.should_run(climb_id):
                        self.logger.info(
                            "Criteria not met for sample %s; acknowledging message",
                            climb_id,
                        )
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
                    try:
                        self._runner.run_pipeline(
                            pipeline=pipeline,
                            sample_id=climb_id,
                            job_uuid=job_uuid,
                            worker_work_dir=self.work_dir,
                            worker_output_dir=self.output_dir,
                        )
                    except RetryablePipelineError as e:
                        if current_attempt >= total_attempts:
                            self._retry_counts.pop(climb_id, None)
                            raise RuntimeError(
                                "Pipeline retries exhausted"
                            ) from e

                        next_attempt = current_attempt + 1
                        self.logger.warning(
                            "Retrying pipeline %s for sample %s (next attempt %d/%d)",
                            pipeline.config.name,
                            climb_id,
                            next_attempt,
                            total_attempts,
                        )
                        self.on_retry(message)
                        continue
                    except NonRetryablePipelineError as e:
                        self._retry_counts.pop(climb_id, None)
                        raise RuntimeError(
                            "Non-retryable pipeline error"
                        ) from e

                    self.logger.info(
                        "Pipeline %s succeeded for sample %s",
                        pipeline.config.name,
                        climb_id,
                    )
                    self._retry_counts.pop(climb_id, None)
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

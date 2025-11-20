import json
import logging
import time
from abc import ABC, abstractmethod
from multiprocessing.synchronize import Event
from pathlib import Path
from typing import Any

from cherami.pipeline_runner import PipelineRunner
from cherami.pipelines import Pipeline
from cherami.utils import init_kubernetes, init_varys


class Worker(ABC):
    """Defines a base template for all workers, consuming a RabbitMQ queue and launching pipelines.

    The base class implements orchestration methods common to every worker. It
    binds to the configured exchange, loads an associated Pipelines config, and executes
    pipelines via the `PipelineRunner` module. Subclasses expose the pipeline they run via the `pipeline`
    property. However, subclasses can optionally override the `on_skip`, `on_success`, `on_retry`,
    and `on_catastrophic_error` methods to define additional behaviour when a these events happen.

    For example, a subclass could override `on_success` to push a payload to another message queue, or
    override `on_catastrophic_error` to send an alert to an error message queue.

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
        *,
        worker_name: str,
        listen_exchange: str,
        listen_queue_suffix: str,
        varys_config_path: Path,
        varys_log_path: Path,
        publish_queue_suffix: str | None = None,
        publish_exchange: str | None = None,
    ) -> None:
        self.worker_name: str = worker_name
        self.listen_exchange: str = listen_exchange
        self.listen_queue_suffix: str = listen_queue_suffix
        self.varys_config_path: Path = varys_config_path
        self.varys_log_path: Path = varys_log_path
        self.publish_queue_suffix: str | None = publish_queue_suffix
        self.publish_exchange: str | None = publish_exchange
        self._varys_client: Any
        self._runner: PipelineRunner
        self.logger = logging.getLogger(f"cherami.workers.{worker_name}")
        self._retry_counts: dict[str, int] = {}

    def on_skip(self, message: Any) -> None:
        """Called when a message is to be skipped. By default will acknowledge the provided message.

        Called when `pipeline.should_run(sample_id)` returns `False`, indicating the sample should not be processed.
        The default implementation acknowledges the message and moves on. Override this if you need custom behavior
        when skipping samples, such as logging to a separate queue etc etc

        Args:
            message: Varys message representing a sample that should NOT launch a pipeline.
        """
        self._varys_client.acknowledge_message(message)

    def on_success(self, message: Any) -> None:
        """Called when a pipeline run completes successfully.

        The default implementation publishes the message to `publish_exchange`/`publish_queue_suffix`
        (if configured) and then acknowledges the original message. Publishing to another queue allows
        chaining workers together: one worker's `publish_queue_suffix` becomes the next worker's
        `listen_queue_suffix`.

        Override this if you need additional logic on success, e.g. triggering downstream notifications etc

        Args:
            message: Varys message representing a sample that completed successfully.
        """
        ## if a worker configured a publish queue, this  send that message to the listen_exchange,
        ## unless the worker ALSO configures a publish_exchange, in which case use that
        if self.publish_queue_suffix:
            target_exchange = self.publish_exchange or self.listen_exchange
            self._varys_client.send(
                message=message.body,
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
        the count reaches `pipeline.config.max_retries`, the worker calls `on_catastrophic_error` instead.

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

        The default implementation acknowledges the message to remove it from the queue, preventing it
        from being reprocessed indefinitely.

        Override this if you need to handle failures specially, such as logging to an error
        queue, sending alerts, or writing to a dead letter queue.

        Args:
            message: Varys message representing a sample that has failed.
        """
        self._varys_client.acknowledge_message(message)
        ## TODO: consider publishing to an error queue if configured

    @property
    @abstractmethod
    def pipeline(self) -> Pipeline:
        """Pipeline instance that this worker will run.

        Subclasses MUST implement this property to bind a worker to a specific pipeline.

        Returns:
            An instance of a `BasePipeline` subclass for a given pipeline.
        """

    def run(self, sample_log: Path, shutdown_event: Event) -> None:
        """Main worker loop, consuming messages and launching pipelines as required.

        Main entry point for the worker when spawned. This is a long-running method that will listen for messages on
        the configured Varys exchange/queue, and launch pipelines as required.

        Args:
            sample_log: Path to sample log.
            shutdown_event: Shutdown event
        """
        self._varys_client = init_varys(self.varys_config_path, self.varys_log_path)
        self._runner = PipelineRunner(
            k8_api=init_kubernetes(),
            sample_log=sample_log,
        )
        pipeline = self.pipeline
        message = None
        try:
            while not shutdown_event.is_set():
                ## the basic flow  of a worker is first check first for any messages - listening to varys is blocking.
                ## If there are no messages after the timeout, poll again and wait. If there is a message, parse it to
                ## get sample_id and uuid and call the `should_run` method on the pipeline to see if passess decision
                ## logic. If it does not pass, call `on_skip` to ack and move to next sample. If it does pass, call
                ## `run_pipeline` on the `PipelineRunner` instance to then launch the pipeline. The result of that call
                ## indicates success/failure. If success, call `on_success` to ack and potentially publish to next
                ## queue. If failure, it will be retried up to `max_retries`, calling `on_retry` to nack the message so
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

                    payload = json.loads(message.body)

                    sample_id = payload.get("sample_id")
                    job_uuid = payload.get("uuid")

                    ## TODO: Handle dodgy payloads
                    ## probably ack and log

                    if not pipeline.should_run(sample_id):
                        self.logger.info(
                            "Criteria not met for sample %s; acknowledging message",
                            sample_id,
                        )
                        self.on_skip(message)
                        continue

                    max_retries = pipeline.config.max_retries
                    total_attempts = max_retries + 1
                    current_attempt = self._retry_counts.get(sample_id, 0) + 1
                    self._retry_counts[sample_id] = current_attempt

                    self.logger.info(
                        "Worker running sample %s (attempt %d/%d)",
                        sample_id,
                        current_attempt,
                        total_attempts,
                    )
                    result = self._runner.run_pipeline(
                        pipeline=pipeline,
                        sample_id=sample_id,
                        job_uuid=job_uuid,
                    )

                    if result.success:
                        self.logger.info(
                            "Pipeline %s succeeded for sample %s",
                            pipeline.config.name,
                            sample_id,
                        )
                        self._retry_counts.pop(sample_id, None)
                        self.on_success(message)
                    else:
                        self.logger.error(
                            "Pipeline %s failed for sample %s with errors: %s",
                            pipeline.config.name,
                            sample_id,
                            "|".join(result.errors),
                        )

                        if current_attempt >= total_attempts:
                            self.logger.error(
                                "Pipeline %s exhausted max retries for sample %s",
                                pipeline.config.name,
                                sample_id,
                            )
                            self.logger.error("Catastrophic error for sample %s", sample_id)
                            self._retry_counts.pop(sample_id, None)
                            self.on_sample_failure(message)
                        else:
                            if result.retry:
                                next_attempt = current_attempt + 1
                                self.logger.warning(
                                    "Retrying pipeline %s for sample %s (next attempt %d/%d)",
                                    pipeline.config.name,
                                    sample_id,
                                    next_attempt,
                                    total_attempts,
                                )
                                self.on_retry(message)
                            else:
                                self.logger.error("Catastrophic error for sample %s", sample_id)
                                self.on_sample_failure(message)
                except Exception as e:
                    self.logger.exception("Unhandled exception in worker: %s", str(e))
                    self.on_sample_failure(message)
                    ## TODO: we might consider retrying here if we got a valid message and payload
                    ## possibly an exception might be rasied from something transient like k8s failure
                    ## and so re-running might be a valid option.
        finally:
            self.logger.info("%s worker stopping", self.worker_name)
            self._varys_client.close()

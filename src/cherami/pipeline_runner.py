import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from kubernetes.client.api import BatchV1Api
from kubernetes.client.exceptions import ApiException

from cherami.pipelines.base import Pipeline

logger = logging.getLogger(__name__)


@dataclass()
class PipelineResult:
    """Outcome result for a single pipeline execution.

    Instances are written to the JSONL sample log and used by workers to
    decide whether retries are required via pipelines setting the `retry` flag.
    """

    sample_id: str
    pipeline_name: str
    job_uuid: str
    success: bool
    errors: list[str]
    started_at: datetime
    completed_at: datetime
    retry: bool = False

    def to_json(self) -> dict[str, Any]:
        """Return a subset of fields in JSON format"""
        return {
            "sample_id": self.sample_id,
            "pipeline": self.pipeline_name,
            "job_uuid": self.job_uuid,
            "success": self.success,
            "errors": self.errors,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
        }


class PipelineRunner:
    """Run Nextflow pipelines by executing Kubernetes Jobs.

    Workers delegate pipeline execution to this class. It takes a
    `BasePipeline` configuration and uses it to create a Kubernetes Job,
    and waits until the run completes. Ultimately returns a `PipelineResult` which is
    used by the worker to handle success, retry, or failure states.
    """

    def __init__(
        self,
        *,
        k8_api: BatchV1Api,
        sample_log: Path,
    ) -> None:
        self.k8_api = k8_api
        self._sample_log = sample_log
        logger.info("Initialised pipeline runner")

    def run_pipeline(self, *, pipeline: Pipeline, sample_id: str, job_uuid: str) -> PipelineResult:
        """Launch the given pipeline for a specific sample.

        Workers call this method once they have decided a sample should run.
        It validates the pipeline configuration, and wraps `_execute_pipeline`
        for actual job orchestration, and records the outcome. Failed executions mark
        `result.retry = True` so the worker can decide whether to re-queue the
        sample based on configured retry limits.

        Args:
            pipeline: Pipeline instance to run.
            sample_id: Sample identifier to run the pipeline for.
            job_uuid: Unique job UUID for this pipeline run.

        Returns:
            `PipelineResult` instance for the run outcome.

        """
        pipeline.validate()
        started_at = datetime.now()
        success, errors = self._execute_pipeline(pipeline, sample_id, job_uuid)
        completed_at = datetime.now()

        result = PipelineResult(
            sample_id=sample_id,
            pipeline_name=pipeline.config.name,
            job_uuid=job_uuid,
            success=success,
            errors=errors,
            started_at=started_at,
            completed_at=completed_at,
        )

        if not success:
            result.retry = True
            ## TODO: consider what error messages should stop a retry

        self._log_result(result)
        return result

    def _execute_pipeline(self, pipeline: Pipeline, sample_id: str, job_uuid: str) -> tuple[bool, list[str]]:
        """Submit the Kubernetes Job and return the result of execution.

        This method generates the samplesheet and job manifest from the pipeline configuration,
        submits the job to Kubernetes, then enters a loop checking job status every 10
        seconds. The job can complete in three ways: success (pod exits 0 and trace file passes),
        failure (e.g. pod exhausted backoff limit), or timeout (exceeded job_timeout). When the pod
        exits successfully, the method checks the Nextflow trace file to verify all processes
        exited with allowed codes.

        Args:
            pipeline: Pipeline instance to run.
            sample_id: Sample identifier to run the pipeline for.
            job_uuid: Unique job UUID for this pipeline run.

        Returns:
            A tuple containing a boolean indicating success, and a list of error messages.
        """
        job_name = f"{pipeline.config.name}-{job_uuid}"
        errors = []

        try:
            try:
                samplesheet_path = pipeline.generate_samplesheet([sample_id], job_uuid)
            except Exception as e:
                errors.append(f"samplesheet_generation_failed: {e}")
                return False, errors

            job_manifest = pipeline.create_job_manifest(
                samplesheet_path=samplesheet_path,
                job_id=job_uuid,
            )

            logger.info("Creating job %s for pipeline %s", job_name, pipeline.config.name)
            self.k8_api.create_namespaced_job(
                body=job_manifest,
                namespace=pipeline.config.namespace,
            )

            reported_failed_pods = 0

            while True:
                resp = self.k8_api.read_namespaced_job_status(
                    name=job_name,
                    namespace=pipeline.config.namespace,
                )
                status = resp.status  # type: ignore

                if status and status.succeeded:
                    logger.info("k8 job %s completed", job_name)

                    trace_file = pipeline.config.output_dir / job_uuid / "pipeline_trace.txt"

                    if not trace_file.exists():
                        error_msg = f"trace_file_missing: {trace_file}"
                        errors.append(error_msg)
                        logger.error(
                            "Pipeline %s for sample %s missing trace file %s",
                            pipeline.config.name,
                            sample_id,
                            trace_file,
                        )
                        self._cleanup_job(job_name=job_name, pipeline=pipeline)
                        return False, errors

                    success = pipeline.evaluate_exit_status(trace_file)

                    if success:
                        return True, errors

                    error_msg = f"trace_evaluation_failure: Pipeline {pipeline.config.name} processes failed"
                    errors.append(error_msg)
                    logger.error(
                        "Pipeline %s for sample %s failed trace evaluation",
                        pipeline.config.name,
                        sample_id,
                    )
                    self._cleanup_job(job_name=job_name, pipeline=pipeline)
                    return False, errors

                if status and status.failed:
                    failed_count = status.failed
                    if failed_count > reported_failed_pods:
                        logger.error(
                            "k8 job %s pod failed (%d/%d)",
                            job_name,
                            failed_count,
                            pipeline.config.backoff_limit,
                        )
                        reported_failed_pods = failed_count

                    if failed_count >= pipeline.config.backoff_limit:
                        logger.error("k8 job %s exhausted backoff limit", job_name)
                        errors.append(
                            f"pod_failure: Job {job_name} exhausted backoff limit "
                            f"({pipeline.config.backoff_limit} attempts)"
                        )
                        self._cleanup_job(job_name=job_name, pipeline=pipeline)
                        return False, errors

                if status.start_time and time.time() - status.start_time.timestamp() > pipeline.config.job_timeout:
                    logger.error("k8 job %s timed out", job_name)
                    errors.append(f"pod_failure: Job {job_name} timed out after {pipeline.config.job_timeout} seconds")
                    self._cleanup_job(job_name=job_name, pipeline=pipeline)
                    return False, errors

                logger.debug("k8 job %s still running...", job_name)
                time.sleep(10)

        except Exception as e:
            error_msg = f"exception: A pipeline failed with an unhandled exception: {e}"
            errors.append(error_msg)
            logger.exception(
                "Exception running pipeline %s for sample %s",
                pipeline.config.name,
                sample_id,
            )
            self._cleanup_job(job_name=job_name, pipeline=pipeline)

        return False, errors

    def _cleanup_job(self, *, job_name: str, pipeline: Pipeline) -> None:
        """Delete a Kubernetes Job and wait for deletion to complete.

        Kubernetes job deletion is asynchronous, so this method polls for up to 60 seconds
        until the job returns a 404 status, indicating it has been removed. This wait
        is necessary because attempting to create a new job with the same name before deletion
        completes will result in a 409 error. If deletion times out, subsequent retry
        attempts for the same sample may fail with name collisions.

        Args:
            job_name: Name of the Kubernetes Job to delete.
            pipeline: Pipeline instance the job belongs to.
        """
        try:
            self.k8_api.delete_namespaced_job(
                name=job_name,
                namespace=pipeline.config.namespace,
                propagation_policy="Foreground",
            )
        except ApiException as e:
            if e.status != 404:
                raise

        ## k8 can sometimes take a while to delete jobs, so waits up to 60 for it to delete
        ## otherwise a new job with the same uuid cant be created (will get error 409)
        deadline = time.time() + 60
        while True:
            try:
                self.k8_api.read_namespaced_job_status(
                    name=job_name,
                    namespace=pipeline.config.namespace,
                )
            except ApiException as e:
                ## if its 404 I guess we can assume its deleted
                if e.status == 404:
                    return
                raise
            if time.time() >= deadline:
                ## TODO: if this happens strong chance that re-running will raise exception 409 - how to handle?
                logger.warning("Timed out waiting for job %s to delete", job_name)
                return

    def _log_result(self, result: PipelineResult) -> None:
        """Append a JSONL record describing the run outcome."""
        if not self._sample_log:
            return

        try:
            self._sample_log.parent.mkdir(parents=True, exist_ok=True)
            with self._sample_log.open("a", encoding="utf-8") as file_handle:
                file_handle.write(json.dumps(result.to_json()))
                file_handle.write("\n")
        except Exception:
            logger.error("Failed writing pipeline log for sample %s", result.sample_id)

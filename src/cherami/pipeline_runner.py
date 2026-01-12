import logging
import time
from pathlib import Path

from kubernetes.client.api import BatchV1Api
from kubernetes.client.exceptions import ApiException
from onyx.exceptions import OnyxConnectionError

from cherami.pipelines import Pipeline

logger = logging.getLogger(__name__)


class RetryablePipelineError(RuntimeError):
    """Pipeline error eligible for retry."""


class NonRetryablePipelineError(RuntimeError):
    """Pipeline error not eligible for retry."""


class PipelineRunner:
    """Run Nextflow pipelines by executing Kubernetes Jobs.

    Workers use this class for pipeline execution. It takes a `BasePipeline`
    configuration and uses it to create a Kubernetes Job, and waits until the run
    completes. The worker handles success, retry, or failure states based on
    exceptions raised from execution.
    """

    def __init__(
        self,
        *,
        k8_api: BatchV1Api,
    ) -> None:
        self.k8_api = k8_api

    def run_pipeline(
        self,
        *,
        pipeline: Pipeline,
        sample_id: str,
        job_uuid: str,
        worker_work_dir: Path,
        worker_output_dir: Path,
    ) -> None:
        """Launch the given pipeline for a specific sample.

        Workers call this method once they have decided a sample should run.
        It validates the pipeline configuration, and wraps `_execute_pipeline`
        for actual job orchestration.

        Args:
            pipeline: Pipeline instance to run.
            sample_id: Sample identifier to run the pipeline for.
            job_uuid: Unique job UUID for this pipeline run.
            worker_work_dir: Output directory for intermediate files.
            worker_output_dir: Output directory for published outputs.

        Raises:
            RetryablePipelineError: When a failure is eligible for retry.
            NonRetryablePipelineError: For non-retryable pipeline failures.
        """
        pipeline.validate()
        self._execute_pipeline(
            pipeline=pipeline,
            sample_id=sample_id,
            job_uuid=job_uuid,
            worker_work_dir=worker_work_dir,
            worker_output_dir=worker_output_dir,
        )

    def _cleanup(self, *, job_name: str, pipeline: Pipeline) -> None:
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
                logger.warning(
                    "Timed out waiting for job %s to delete", job_name
                )
                return

    def _evaluate_trace(
        self,
        *,
        pipeline: Pipeline,
        sample_id: str,
        job_name: str,
        job_dirs: dict[str, Path],
    ) -> bool:
        trace_file = job_dirs["output_dir"] / "pipeline_trace.txt"

        if not trace_file.exists():
            logger.error(
                "Pipeline %s for sample %s missing trace file %s",
                pipeline.config.name,
                sample_id,
                trace_file,
            )
            logger.info(
                "Job for %s (%s) failed",
                sample_id,
                job_name,
            )
            raise NonRetryablePipelineError(
                f"trace_file_missing: {trace_file}"
            )

        success = pipeline.evaluate_exit_status(trace_file)

        if success:
            return True

        logger.error(
            "Pipeline %s for sample %s failed trace evaluation",
            pipeline.config.name,
            sample_id,
        )
        logger.info(
            "Job for %s (%s) failed",
            sample_id,
            job_name,
        )
        raise NonRetryablePipelineError(
            f"trace_evaluation_failure: Pipeline {pipeline.config.name} processes failed"
        )

    def _poll_job(
        self,
        *,
        pipeline: Pipeline,
        job_name: str,
    ) -> str:
        reported_failed_pods = 0

        while True:
            resp = self.k8_api.read_namespaced_job_status(
                name=job_name,
                namespace=pipeline.config.namespace,
            )
            status = resp.status  # type: ignore

            if status and status.succeeded:
                return "succeeded"

            if status and status.failed:
                failed_count = status.failed
                if failed_count > reported_failed_pods:
                    logger.warning(
                        "k8 job %s pod failed (%d/%d)",
                        job_name,
                        failed_count,
                        pipeline.config.backoff_limit,
                    )
                    reported_failed_pods = failed_count

                if failed_count >= pipeline.config.backoff_limit:
                    logger.warning(
                        "k8 job %s exhausted backoff limit", job_name
                    )
                    raise RetryablePipelineError(
                        f"pod_failure: Job {job_name} exhausted backoff limit "
                        f"({pipeline.config.backoff_limit} attempts)"
                    )

            if (
                status.start_time
                and time.time() - status.start_time.timestamp()
                > pipeline.config.job_timeout
            ):
                logger.error("k8 job %s timed out", job_name)
                raise RetryablePipelineError(
                    f"pod_failure: Job {job_name} timed out after "
                    f"{pipeline.config.job_timeout} seconds"
                )

            logger.debug("k8 job %s still running...", job_name)
            time.sleep(10)

    def _create_job(
        self,
        *,
        pipeline: Pipeline,
        job_name: str,
        sample_id: str,
        job_uuid: str,
        job_dirs: dict[str, Path],
    ) -> None:
        ## to handle situations where a worker crashes with jobs pending, here poll for an existing job with same
        ## name, if this returns something we can assume the job is already running or has completed within the k8
        ## TTL window so can "re-attach" to it and move straight to the validation loop. If the job isnt found, it
        ## is created like normal
        jobs = self.k8_api.list_namespaced_job(
            namespace=pipeline.config.namespace,
            field_selector=f"metadata.name={job_name}",
            limit=1,
        )
        job_exists = bool(jobs.items)

        if not job_exists:
            pipeline.generate_samplesheet(
                [sample_id],
                job_uuid,
                job_dirs["samplesheet_path"],
            )

            job_manifest = pipeline.create_job_manifest(
                job_id=job_uuid,
                climb_id=sample_id,
                job_dirs=job_dirs,
            )

            logger.info(
                "Creating job for %s job name: %s",
                sample_id,
                job_name,
            )
            try:
                self.k8_api.create_namespaced_job(
                    body=job_manifest,
                    namespace=pipeline.config.namespace,
                )
            except ApiException as e:
                ## if somehow the first check missed an existing job capture the 409 error here and let it continue
                ## to the validation loop, otherwise raise any other exceptions
                if e.status == 409:
                    logger.warning(
                        "Job %s already exists; attaching to existing run",
                        job_name,
                    )
                else:
                    raise
        else:
            logger.info(
                "Attaching to existing job %s",
                job_name,
            )

    def _create_dirs(
        self, sample_id: str, worker_work_dir: Path, worker_output_dir: Path
    ) -> dict[str, Path]:
        ## creates all the dirs to run a sample
        sample_work_dir = worker_work_dir / sample_id
        sample_output_dir = worker_output_dir / sample_id

        nxf_work_dir = sample_work_dir / f"{sample_id}_nxf_work"
        nxf_home_dir = worker_work_dir / ".nextflow"
        nxf_log_file = sample_work_dir / f"{sample_id}.log"

        samplesheet_path = sample_work_dir / f"{sample_id}_samplesheet.csv"

        worker_work_dir.mkdir(parents=True, exist_ok=True)
        worker_output_dir.mkdir(parents=True, exist_ok=True)
        sample_work_dir.mkdir(parents=True, exist_ok=True)
        nxf_work_dir.mkdir(parents=True, exist_ok=True)
        nxf_home_dir.mkdir(parents=True, exist_ok=True)
        sample_output_dir.mkdir(parents=True, exist_ok=True)

        return {
            "working_dir": sample_work_dir,
            "output_dir": sample_output_dir,
            "nxf_work_dir": nxf_work_dir,
            "nxf_home_dir": nxf_home_dir,
            "nxf_log_file": nxf_log_file,
            "samplesheet_path": samplesheet_path,
        }

    def _execute_pipeline(
        self,
        pipeline: Pipeline,
        sample_id: str,
        job_uuid: str,
        worker_work_dir: Path,
        worker_output_dir: Path,
    ) -> None:
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
            worker_work_dir: Output directory for intermediate files.
            worker_output_dir: Output directory for published outputs.

        Raises:
            RetryablePipelineError: When a failure is eligible for retry.
            NonRetryablePipelineError: For non-retryable pipeline failures.
        """
        job_name = f"{pipeline.config.name}-{job_uuid}"
        job_dirs = self._create_dirs(
            sample_id, worker_work_dir, worker_output_dir
        )

        try:
            self._create_job(
                pipeline=pipeline,
                job_name=job_name,
                sample_id=sample_id,
                job_uuid=job_uuid,
                job_dirs=job_dirs,
            )

            status = self._poll_job(
                pipeline=pipeline,
                job_name=job_name,
            )

            if status == "succeeded":
                self._evaluate_trace(
                    pipeline=pipeline,
                    sample_id=sample_id,
                    job_name=job_name,
                    job_dirs=job_dirs,
                )
                logger.info(
                    "Job for %s (%s) completed successfully",
                    sample_id,
                    job_name,
                )
                return

            raise RuntimeError("Pipeline execution failed")

        except OnyxConnectionError as e:
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job %s", job_name)
            raise RetryablePipelineError(
                "Onyx connection error running pipeline"
            ) from e

        except ApiException as e:
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job %s", job_name)
            raise RetryablePipelineError(
                "Kubernetes API error running pipeline"
            ) from e

        except (RetryablePipelineError, NonRetryablePipelineError):
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job %s", job_name)
            raise

        except Exception as e:
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job %s", job_name)
            raise NonRetryablePipelineError("Pipeline execution failed") from e

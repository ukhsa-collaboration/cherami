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

    Orchestrates the execution of pipelines by creating and monitoring
    Kubernetes Jobs. It handles job submission, status polling, and
    result verification via Nextflow trace files. It also manages the
    creation of necessary directory structures and samplesheets.
    """

    def __init__(
        self,
        *,
        k8_api: BatchV1Api,
    ) -> None:
        self.k8_api = k8_api

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

        Raises:
            ApiException: If the Kubernetes API call fails with a status other
                than 404.
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

        ## k8 can sometimes take a while to delete jobs, so waits up to 180s for it to delete
        ## otherwise a new job with the same uuid cant be created (will get error 409)
        deadline = time.time() + 180
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
                raise TimeoutError(
                    f"Timed out waiting for job {job_name} to delete"
                )

    def _evaluate_trace(
        self,
        *,
        pipeline: Pipeline,
        sample_id: str,
        job_name: str,
        job_dirs: dict[str, Path],
    ) -> bool:
        """Verify pipeline success by examining the Nextflow trace file.

        Checks that the trace file exists in the output directory (extracted from
        `job_dirs`) and that all processes listed in it exited successfully.

        Args:
            pipeline: Pipeline instance being evaluated.
            sample_id: Sample identifier.
            job_name: Kubernetes Job name (for logging).
            job_dirs: Dictionary containing run directories, specifically "output_dir".

        Returns:
            True if the trace file exists and indicates success.

        Raises:
            NonRetryablePipelineError: If the trace file is missing or if any
                process in the trace indicates failure.
        """
        trace_file = job_dirs["output_dir"] / "pipeline_trace.txt"

        if not trace_file.exists():
            raise NonRetryablePipelineError("trace_file_missing")

        success = pipeline.evaluate_exit_status(trace_file)

        if not success:
            raise NonRetryablePipelineError("trace_evaluation_failure")
        return True

    def _poll_job(
        self,
        *,
        pipeline: Pipeline,
        job_name: str,
    ) -> str:
        """Monitor the status of a running Kubernetes job.

        Polls the job status every 10 seconds. It checks for successful completion,
        pod failures (exhausting backoff limits), and timeouts.

        Args:
            pipeline: Pipeline instance determining timeout and backoff limits.
            job_name: Name of the Kubernetes Job to monitor.

        Returns:
            "succeeded" if the job completes successfully.

        Raises:
            RetryablePipelineError: If the job fails (pods crash repeatedly) or
                times out.
        """
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
                        "k8 pod failed (%d/%d)",
                        failed_count,
                        pipeline.config.backoff_limit,
                    )
                    reported_failed_pods = failed_count

                if failed_count >= pipeline.config.backoff_limit:
                    raise RetryablePipelineError(
                        "pod_failure_backoff_limit_exceeded: "
                        f"backoff_limit={pipeline.config.backoff_limit}"
                    )

            if (
                status.start_time
                and time.time() - status.start_time.timestamp()
                > pipeline.config.job_timeout
            ):
                raise RetryablePipelineError(
                    "pod_failure_timeout: "
                    f"timeout_seconds={pipeline.config.job_timeout}"
                )

            logger.debug("k8 job still running...")
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
        """Create and submit a Kubernetes Job for the pipeline.

        Checks if a job with the same name already exists. If not, it uses
        `pipeline.create_job_manifest` (passing `job_dirs`) to generate the
        job spec and submits it.

        Args:
            pipeline: Pipeline instance to run.
            job_name: Name for the Kubernetes Job.
            sample_id: Sample identifier.
            job_uuid: Unique job UUID.
            job_dirs: Dictionary containing all required run directories.

        Raises:
            ApiException: If job creation fails for reasons other than 409 Conflict
                (which is handled gracefully as an attachment to an existing run).
        """
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
        """Create the directory structure required for a pipeline run.

        Sets up the working directory, output directory, Nextflow specific
        directories, and paths for logs and samplesheets.

        Args:
            sample_id: Sample identifier.
            worker_work_dir: Base directory for intermediate work files.
            worker_output_dir: Base directory for final outputs.

        Returns:
            A dictionary containing paths for:
            - working_dir: Sample-specific working directory
            - output_dir: Sample-specific output directory
            - nxf_work_dir: Nextflow work directory
            - nxf_home_dir: Nextflow home directory (shared)
            - nxf_log_file: Path for the Nextflow log file
            - samplesheet_path: Path for the generated samplesheet
        """
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

        Generates configuration, submits the job, and polls for completion.
        Verifies success by checking both pod exit status and the Nextflow
        trace file.

        Args:
            pipeline: Pipeline instance to run.
            sample_id: Sample identifier.
            job_uuid: Unique job UUID.
            worker_work_dir: Output directory for intermediate files.
            worker_output_dir: Output directory for published outputs.

        Raises:
            RetryablePipelineError: For failures eligible for retry (e.g., API errors,
                pod crashes, timeouts).
            NonRetryablePipelineError: For failures not eligible for retry (e.g.,
                trace validation failure).
        """
        job_name = f"{pipeline.config.name}-{job_uuid}"
        sample_output_dir = worker_output_dir / sample_id
        completion_marker = sample_output_dir / ".cherami_complete"
        if completion_marker.exists():
            raise NonRetryablePipelineError("pipeline_already_completed")
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
                completion_marker.write_text(job_uuid)
                logger.info(
                    "Job for %s (%s) completed successfully",
                    sample_id,
                    job_name,
                )
                return

            raise RuntimeError("pipeline_execution_failed")

        except OnyxConnectionError as e:
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job")
            raise RetryablePipelineError("onyx_connection_error") from e

        except ApiException as e:
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job")
            raise RetryablePipelineError("kubernetes_api_error") from e

        except (RetryablePipelineError, NonRetryablePipelineError):
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job")
            raise

        except Exception as e:
            try:
                self._cleanup(job_name=job_name, pipeline=pipeline)
            except Exception:
                logger.exception("Failed to cleanup job")
            raise NonRetryablePipelineError(str(e)) from e

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

        Workers call this method to initiate processing. It validates the pipeline
        configuration and wraps the execution logic.

        Args:
            pipeline: Pipeline instance to run.
            sample_id: Unique identifier for the sample.
            job_uuid: Unique UUID for this pipeline run (from match_uuid in payload).
            worker_work_dir: Directory for intermediate files and Nextflow work.
            worker_output_dir: Directory for final published outputs.

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

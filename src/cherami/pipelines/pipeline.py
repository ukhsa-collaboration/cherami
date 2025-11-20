import csv
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration describing how to run a pipeline."""

    ## general
    name: str
    version: str
    path: str
    ## compute
    cpus: int
    mem: str
    cpu_limit: int
    mem_limit: str
    ## nf configs
    nf_config_path: Path
    nf_profiles: list[str]
    nf_extra_args: list[str]
    work_dir: Path
    output_dir: Path
    ## k8 configs
    namespace: str
    container: str
    backoff_limit: int
    max_retries: int
    retry_timeout: int
    job_timeout: int


class Pipeline(ABC):
    """Abstract base class for pipelines. All pipelines must inherit from this.

    Subclasses must include a `PipelineConfig` via the `config` property and provide a
    `generate_samplesheet` implementation that prepares their inputs.
    """

    pipeline_name: str

    @property
    @abstractmethod
    def config(self) -> PipelineConfig:
        """Configuration required to run a pipeline job.

        Returns:
            A `PipleineConfig` object containing the execution settings for the pipeline.
        """

    @property
    def proc_names(self) -> dict[str, list[int]]:
        """Optional mapping of Nextflow process names to their allowed exit codes.

        Returns:
            Process-specific allowed exit codes used when evaluating trace files. When
            empty, every process must exit with code 0.
        """
        return {}

    @abstractmethod
    def generate_samplesheet(self, samples: list[str], job_id: str) -> Path | None:
        """Creates a samplesheet for the provided sample IDs.

        Implementations should create a samplesheet file for all samples being input into the pipeline.
        Samplesheet format is pipeline-specific. The returned path will be passed to the pipeline's job to be
        used as input.

        Arguments:
            samples: Sample identifiers the pipeline will process.
            job_id: Identifier associated with the orchestrated job. Implementations can
                use this to name any generated files.

        Returns:
            Path to the generated samplesheet, or `None` when no samplesheet
            is required.
        """

    def _check_paths(self) -> None:
        """Log warnings whenever configured filesystem locations are missing."""
        if not self.config.work_dir.exists():
            logger.warning("Configured work_dir '%s' does not exist", self.config.work_dir)

        if not self.config.output_dir.exists():
            logger.warning("Configured output_dir '%s' does not exist", self.config.output_dir)

        if not self.config.nf_config_path.exists():
            logger.warning("Configured nf_config_path '%s' does not exist", self.config.nf_config_path)

    def validate(self) -> None:
        """Run validation on the pipeline configuration."""
        self._check_paths()

    ## inspired by https://github.com/CLIMB-TRE/roz/blob/bd0ec88b29f9fd0fc18ca1cc500ad385128c121a/roz_scripts/mscape/mscape_ingest_validation.py#L997
    def evaluate_exit_status(self, trace_file: Path) -> bool:
        """Ensures processes recorded in a Nextflow trace file all exited with allowed codes.

        This uses the `proc_names` property to determine which processes to check and their
        allowed exit codes. If `proc_names` is empty, all processes must have exited with
        code 0.

        Arguments:
            trace_file: Path to the tab-delimited Nextflow trace file for a job run.

        Returns:
            `True` if the trace file exists and every relevant process satisfies its
            defined exit code, otherwise `False`.
        """
        if not trace_file.exists():
            ## TODO: do we re-queue the job if trace file not found?
            ## Decide on wider retry strategy
            return False

        try:
            with trace_file.open("r") as trace_fh:
                reader = csv.DictReader(trace_fh, delimiter="\t")
                ## by default check all processes for exit code 0
                if not self.proc_names:
                    for row in reader:
                        if row["exit"] != "0":
                            logger.warning(
                                "Process %s failed with exit code %s",
                                row["name"],
                                row["exit"],
                            )
                            return False
                    return True
                ## if proc_names provided - determine allowed exit codes per process
                ## this also allows you to only check a subset of processes if you want
                for row in reader:
                    if row["name"] in self.proc_names:
                        allowed_exit_codes = self.proc_names[row["name"]]
                        if int(row["exit"]) not in allowed_exit_codes:
                            logger.warning(
                                "Process %s failed with exit code %s",
                                row["name"],
                                row["exit"],
                            )
                            return False
                return True
        except FileNotFoundError:
            return False

    def should_run(self, sample_id: str) -> bool:
        """Determine whether the pipeline should run for the given sample.

        The default implementation always returns True. Override this to implement decision logic based
        on sample metadata. For example, query onyx to check if the sample has sufficient read count.
        When this returns False, the worker calls `on_skip()` instead of launching the pipeline.

        Arguments:
            sample_id: Identifier provided by the upstream system.

        Returns:
            `True` when the pipeline should run, otherwise `False`.
        """

        return True

    def create_job_manifest(self, samplesheet_path: Path | None, job_id: str) -> dict[str, Any]:
        """Creates the Kubernetes Job manifest for a pipeline run.

        This method constructs a complete Kubernetes Job spec using the pipeline config. The manifest
        includes volume mounts, environment variables (Onyx credentials, AWS config), resource limits,
        and the Nextflow command assembled from config fields. The job runs a single pod. Kubernetes
        will restart the pod up to backoff_limit times if it fails.

        Arguments:
            samplesheet_path: Optional path to a samplesheet to pass to Nextflow via --samplesheet.
            job_id: Unique identifier used for job name and per-job output/work directories.

        Returns:
            Kubernetes Job manifest dictionary to submit via `create_namespaced_job`.
        """
        job_name = f"{self.config.name}-{job_id}"

        job_output_dir = self.config.output_dir / job_id
        nxf_work_dir = self.config.work_dir / job_id
        nxf_home_dir = self.config.work_dir / ".nextflow"

        pod_env_vars = [
            {"name": "NXF_WORK", "value": str(nxf_work_dir)},
            {"name": "NXF_HOME", "value": str(nxf_home_dir)},
            {"name": "ONYX_TOKEN", "value": str(os.environ.get("ONYX_TOKEN"))},
            {"name": "ONYX_DOMAIN", "value": str(os.environ.get("ONYX_DOMAIN"))},
            {"name": "AWS_SECRET_ACCESS_KEY", "value": str(os.environ.get("AWS_SECRET_ACCESS_KEY"))},
            {"name": "AWS_ACCESS_KEY_ID", "value": str(os.environ.get("AWS_ACCESS_KEY_ID"))},
            {"name": "AWS_ENDPOINT_URL", "value": str(os.environ.get("AWS_ENDPOINT_URL"))},
            {
                "name": "AWS_REQUEST_CHECKSUM_CALCULATION",
                "value": str(os.environ.get("AWS_REQUEST_CHECKSUM_CALCULATION")),
            },
        ]

        nextflow_cmd = ["nextflow"]
        nextflow_cmd.extend(["run", str(self.config.path)])

        if self.config.nf_config_path:
            nextflow_cmd.extend(["-c", str(self.config.nf_config_path)])
        if self.config.nf_profiles:
            nextflow_cmd.extend(["-profile", ",".join(self.config.nf_profiles)])
        if self.config.nf_extra_args:
            nextflow_cmd.extend(self.config.nf_extra_args)
        if self.config.output_dir:
            nextflow_cmd.extend(["--outdir", str(job_output_dir)])
        if samplesheet_path:
            nextflow_cmd.extend(["--samplesheet", str(samplesheet_path)])

        command = " ".join(nextflow_cmd)
        logger.debug("Nextflow command: %s", command)

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": job_name,
                "namespace": self.config.namespace,
            },
            "spec": {
                "ttlSecondsAfterFinished": 120,
                "backoffLimit": self.config.backoff_limit,
                "template": {
                    "spec": {
                        "hostname": job_name,
                        "subdomain": self.config.namespace,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 1000,
                            "runAsGroup": 1000,
                            "fsGroup": 1000,
                        },
                        "restartPolicy": "Never",
                        "volumes": [
                            {
                                "name": "shared-public",
                                "persistentVolumeClaim": {"claimName": "cephfs-shared-ro-public"},
                            },
                            {
                                "name": "shared-team",
                                "persistentVolumeClaim": {"claimName": "cephfs-shared-team"},
                            },
                        ],
                        "nodeSelector": {"hub.jupyter.org/node-purpose": "user-compute"},
                        "containers": [
                            {
                                "name": job_name,
                                "image": self.config.container,
                                "resources": {
                                    "requests": {
                                        "cpu": str(self.config.cpus),
                                        "memory": self.config.mem,
                                    },
                                    "limits": {
                                        "cpu": str(self.config.cpu_limit),
                                        "memory": self.config.mem_limit,
                                    },
                                },
                                "volumeMounts": [
                                    {
                                        "mountPath": "/shared/public/",
                                        "name": "shared-public",
                                        "readOnly": True,
                                    },
                                    {
                                        "mountPath": "/shared/team/",
                                        "name": "shared-team",
                                    },
                                ],
                                "workingDir": str(self.config.work_dir),
                                "env": pod_env_vars,
                                "args": ["/bin/sh", "-c", command],
                            },
                        ],
                    },
                },
            },
        }

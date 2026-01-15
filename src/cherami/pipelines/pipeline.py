import csv
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from cherami.config import PipelineConfig

logger = logging.getLogger(__name__)


class Pipeline(ABC):
    """Abstract base class for pipelines.

    All pipelines must inherit from this. Subclasses accept a `PipelineConfig` and
    provide a `generate_samplesheet` implementation that prepares their inputs.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config

    @property
    def proc_names(self) -> dict[str, list[int]]:
        """Optional mapping of Nextflow process names to their allowed exit codes.

        Returns:
            Process-specific allowed exit codes used when evaluating trace files. When
            empty, every process must exit with code 0.
        """
        return {}

    @abstractmethod
    def generate_samplesheet(
        self, samples: list[str], job_id: str, output_filepath: Path
    ) -> None:
        """Creates a samplesheet for the provided sample IDs.

        Implementations should create a samplesheet file for all samples being input into the pipeline.

        Arguments:
            samples: Sample identifiers the pipeline will process.
            job_id: Identifier associated with the orchestrated job.
            output_filepath: Location where the samplesheet should be written.
        """

    def _check_paths(self) -> None:
        """Log warnings whenever configured filesystem locations are missing."""
        if not self.config.nf_config_path.exists():
            logger.warning(
                "Configured nf_config_path '%s' does not exist",
                self.config.nf_config_path,
            )

    def validate(self) -> None:
        """Run validation on the pipeline configuration."""
        self._check_paths()

    ## inspired by https://github.com/CLIMB-TRE/roz/blob/bd0ec88b29f9fd0fc18ca1cc500ad385128c121a/roz_scripts/mscape/mscape_ingest_validation.py#L997
    def evaluate_exit_status(self, trace_file: Path) -> bool:
        """Ensures processes recorded in a Nextflow trace file all exited with allowed
        codes.

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
            with trace_file.open("r") as f:
                reader = csv.DictReader(f, delimiter="\t")
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

    def _get_env_vars(self, job_dirs: dict[str, Path]) -> list[dict[str, str]]:
        """Gets the environment variables required for the Kubernetes pod.

        Arguments:
            job_dirs: Dictionary of filesystem paths used by the job.
        Returns:
            List of environment variable dictionaries in a format for the pod spec.
        """
        required_env_vars = [
            "ONYX_TOKEN",
            "ONYX_DOMAIN",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_ACCESS_KEY_ID",
            "AWS_ENDPOINT_URL",
            "AWS_REQUEST_CHECKSUM_CALCULATION",
        ]
        missing_env_vars = [
            name for name in required_env_vars if name not in os.environ
        ]
        if missing_env_vars:
            missing_vars_display = ", ".join(missing_env_vars)
            raise RuntimeError(
                f"Missing required environment variables: {missing_vars_display}"
            )

        pod_env_values = [
            ("NXF_WORK", job_dirs["nxf_work_dir"]),
            ("NXF_HOME", job_dirs["nxf_home_dir"]),
            ("NXF_LOG_FILE", job_dirs["nxf_log_file"]),
        ]
        pod_env_values.extend(
            (name, os.environ[name]) for name in required_env_vars
        )
        return [
            {"name": name, "value": str(value)}
            for name, value in pod_env_values
        ]

    def create_job_manifest(
        self,
        job_id: str,
        climb_id: str,
        job_dirs: dict[str, Path],
    ) -> dict[str, Any]:
        """Creates the Kubernetes Job manifest for a pipeline run.

        This method constructs a complete Kubernetes Job spec using the pipeline config. The manifest
        includes volume mounts, environment variables (Onyx credentials, AWS config), resource limits,
        and the Nextflow command assembled from config fields. The job runs a single pod. Kubernetes
        will restart the pod up to backoff_limit times if it fails.

        Arguments:
            job_id: UUID associated with the sample.
            climb_id: Climb id for a sample.
            job_dirs: Dictionary of filesystem paths used by the job.

        Returns:
            Kubernetes Job manifest dictionary to submit via `create_namespaced_job`.
        """
        job_name = f"{self.config.name}-{job_id}"

        pod_env_vars = self._get_env_vars(job_dirs)

        nextflow_cmd = ["nextflow"]
        nextflow_cmd.extend(["run", str(self.config.path)])

        if self.config.nf_config_path:
            nextflow_cmd.extend(["-c", str(self.config.nf_config_path)])
        if self.config.nf_profiles:
            nextflow_cmd.extend(
                ["-profile", ",".join(self.config.nf_profiles)]
            )
        if self.config.nf_extra_args:
            nextflow_cmd.extend(self.config.nf_extra_args)
        nextflow_cmd.extend(["--outdir", str(job_dirs["output_dir"])])
        if job_dirs.get("samplesheet_path"):
            nextflow_cmd.extend(
                ["--samplesheet", str(job_dirs["samplesheet_path"])]
            )

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
                                "persistentVolumeClaim": {
                                    "claimName": "cephfs-shared-ro-public"
                                },
                            },
                            {
                                "name": "shared-team",
                                "persistentVolumeClaim": {
                                    "claimName": "cephfs-shared-team"
                                },
                            },
                        ],
                        "nodeSelector": {
                            "hub.jupyter.org/node-purpose": "user-compute"
                        },
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
                                "workingDir": str(job_dirs["working_dir"]),
                                "env": pod_env_vars,
                                "args": ["/bin/sh", "-c", command],
                            },
                        ],
                    },
                },
            },
        }

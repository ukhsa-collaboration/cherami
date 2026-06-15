import csv
import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from pathlib import Path
from typing import Any

from cherami.config import GlobalConfig, PipelineConfig

logger = logging.getLogger(__name__)


class PipelineContext:
    """Object to store the pipeline context."""

    def __init__(
        self, payload: Any, server: str, pipeline_version: str
    ) -> None:
        """
        Attributes:
            payload: dict of the json payload sent in the message.
            server: server for the onyx database
            pipeline_version: version of the current pipeline.
            climb_id: current sample ID, parsed from payload
            job_uuid: - The job UUID (match_uuid).

            onyx_versions_hash: part of the upstream context - set only with
                instance method set_upstream_context_hash().
            orange_box_version: part of the upstream context - not set on init.

        Raises:
            ValueError: If the payload is missing required fields.
        """
        # shared attributes:
        self.payload: dict[str, Any] = payload
        self.server: str = server
        self.pipeline_version: str = pipeline_version

        self.climb_id: str
        self.job_uuid: str

        self.onyx_versions_hash: str | None = None
        self.orange_box_version: str | None = None

        try:
            self.climb_id = self.payload["climb_id"]
            self.job_uuid = self.payload["match_uuid"]
        except KeyError as k:
            raise ValueError(f"Message missing {k}") from k

    def get_upstream_context_hash(self) -> str:
        """
        Query Onyx for the upstream context and calculate the hash, store in
        attribute.

        Returns:
            - string - onyx versions hash.

        Raises:
            - RuntimeError - if onyx cannot be reached.
        """
        from onyx_analysis_helper import onyx_analysis_helper_functions as oa

        _, current_onyx_versions, exitcode = (
            oa.get_data_and_versions_from_onyx(
                sample_id=self.climb_id,
                server=self.server,
                fields=["climb_id"],
            )
        )

        if exitcode != 0:
            logger.error(
                "Cannot query Onyx for upstream context, see previous "
                "logs for reason."
            )
            raise RuntimeError(
                "Onyx cannot be queried for upstream context - check logs."
            )

        return oa._calculate_versions_hash(current_onyx_versions)


class Pipeline(ABC):
    """Base class for pipelines.

    Subclasses must implement `generate_samplesheet` and may override other methods.
    """

    def __init__(
        self, config: PipelineConfig, global_config: GlobalConfig
    ) -> None:
        self.config: PipelineConfig = config
        self.global_config: GlobalConfig = global_config

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
        """Writes a samplesheet for the provided sample IDs to `output_filepath`.

        Implementations should create a samplesheet with all input fields required
        for the nextflow pipeline. Typically this will involve querying Onyx. The
        samplesheet should be written to `output_filepath`.

        Arguments:
            samples: Sample identifiers the pipeline will process.
            job_id: Identifier associated with the orchestrated job.
            output_filepath: Location where the samplesheet should be written.

        Raises:
            OSError: If the samplesheet fails to write.
        """

    def build_context(self, payload: Any) -> PipelineContext:
        """
        Build the context for the pipeline.
        Overwrite this class to add additional attributes for the context.

        Returns: PipelineContext object.

        """
        return PipelineContext(
            payload=payload,
            server=self.global_config.server,
            pipeline_version=self.config.version,
        )

    def _check_paths(self) -> None:
        """Logs warnings for missing configured paths."""
        if not self.config.nf_config_path.exists():
            logger.warning(
                "Configured nf_config_path '%s' does not exist",
                self.config.nf_config_path,
            )

    def validate(self) -> None:
        """Run validation checks on the pipeline configuration before execution."""
        self._check_paths()

    ## inspired by https://github.com/CLIMB-TRE/roz/blob/bd0ec88b29f9fd0fc18ca1cc500ad385128c121a/roz_scripts/mscape/mscape_ingest_validation.py#L997
    def evaluate_exit_status(self, trace_file: Path) -> bool:
        """Returns True when the Nextflow trace indicates allowed process exits.

        When `self.proc_names` is empty, all processes must exit with code 0.

        Arguments:
            trace_file: Path to the tab-delimited Nextflow trace file for a job run.

        Returns:
            `True` if the trace file exists and every relevant process satisfies its
            defined exit code, otherwise `False`.
        """
        if not trace_file.exists():
            logger.error("Trace file %s does not exist.", trace_file)
            return False

        try:
            with trace_file.open("r") as f:
                reader = csv.DictReader(f, delimiter="\t")

                process_exitcodes: defaultdict[str, list[int | str]] = (
                    defaultdict(list)
                )
                for row in reader:
                    # If there is a line of empty 'columns', skip
                    if all(val == "" for val in row.values()):
                        continue
                    try:
                        process_exitcodes[row["name"]].append(int(row["exit"]))
                    except ValueError:
                        logger.warning(
                            "Expected integer-like exitcode in trace"
                            "file for process %s, got %s",
                            row["name"],
                            row["exit"],
                        )
                        process_exitcodes[row["name"]].append(row["exit"])
                    except KeyError as k:
                        logger.error(
                            "Expected to find column %s in trace file. "
                            "Cannot validate processes",
                            k,
                        )
                        return False

                # If the dict is empty, the file is empty (with or without header)
                if not process_exitcodes:
                    logger.error("Trace file %s is empty.", trace_file)
                    return False
                # If proc_names provided - determine allowed exit codes per
                # process deinfe. This allows you to ONLY check a subset of
                # processes if you want
                if self.proc_names:
                    failing_processes: dict[str, list[int | str]] = {}
                    for proc, ec in process_exitcodes.items():
                        if proc in self.proc_names:
                            allowed_ec: set[int] = set(self.proc_names[proc])
                            if not any(e in allowed_ec for e in ec):
                                failing_processes[proc] = ec

                # By default, check all processes contain at least one 0
                # exitcode. This does not assume that the pipeline trace file
                # is added in chronological order, but does assume that a
                # process can't fail AFTER it has completed succesfully
                else:
                    failing_processes: dict[str, list[int | str]] = {
                        proc: ec
                        for proc, ec in process_exitcodes.items()
                        if 0 not in ec
                    }
                # Write to log
                if failing_processes:
                    for proc, ecs in failing_processes.items():
                        logger.error(
                            "Process %s failed with exit code(s) %s",
                            proc,
                            ecs,
                        )
                    return False
                return True

        except FileNotFoundError:
            return False

    def should_run(self, context: PipelineContext) -> bool:
        """Determine whether the pipeline should run for the given sample.

        The default implementation will return true. Override this to implement
        decision logic based on sample metadata.
        When this returns False, the worker calls `on_skip()` instead of launching the pipeline.

        Arguments:
            PipelineContext object: instance of the PipelineObject that houses
            attributes for

        Returns:
            `True` when the pipeline should run, otherwise `False`.
        """
        return True

    def _get_env_vars(self, job_dirs: dict[str, Path]) -> list[dict[str, str]]:
        """Returns environment variables required for the Kubernetes pod spec.

        Arguments:
            job_dirs: Dictionary of filesystem paths used by the job.
        Returns:
            List of environment variable dictionaries in a format for the pod spec.
        Raises:
            RuntimeError: If any required environment variables are missing.
            KeyError: If required paths are missing from `job_dirs`.
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
            ("NXF_WORK", str(job_dirs["nxf_work_dir"])),
            ("NXF_HOME", str(job_dirs["nxf_home_dir"])),
            ("NXF_LOG_FILE", str(job_dirs["nxf_log_file"])),
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
        job_dirs: dict[str, Path],
        annotations: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Returns the Kubernetes Job manifest for a pipeline run.

        Arguments:
            job_id: UUID associated with the sample.
            climb_id: Climb id for a sample.
            job_dirs: Dictionary of filesystem paths used by the job.
            annotations: Optional Kubernetes Job annotations for this run.

        Returns:
            Kubernetes Job manifest dictionary to submit via `create_namespaced_job`.

        Raises:
            RuntimeError: If required environment variables are missing.
            KeyError: If required paths are missing from `job_dirs`.
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

        metadata = {
            "name": job_name,
            "namespace": self.config.namespace,
        }
        if annotations:
            metadata["annotations"] = annotations

        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": metadata,
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


class PathCharPipeline(Pipeline):
    """Template Pipeline object for pathogen characterisation pipelines
    (PathChars)."""

    def build_context(self, payload: dict[str, Any]) -> PipelineContext:
        """
        Overwrite the build_context function. Get the orange_box_version from
        the payload and get the current onyx context. Compare the current onyx
        context with the payload, if these do not match, exit.

        Raises:
            RuntimeError: the upstream context cherami sent does not match the
            current onyx state.
        """
        # Populate the context object
        context: PipelineContext = super().build_context(payload)
        # Add the current onyx versions hash
        context.onyx_versions_hash = context.get_upstream_context_hash()

        # Check the current onyx versions hash matches the one sent in the
        # payload:
        # This checks if state of onyx now is same as in the payload - if
        # message has sat on the queue for a while it _could_ be out of date.
        try:
            if context.onyx_versions_hash != payload["onyx_versions_hash"]:
                logger.debug(
                    "Onyx state out of sync: current onyx state hash: %s, "
                    "message hash: %s",
                    context.onyx_versions_hash,
                    payload["onyx_versions_hash"],
                )
                raise RuntimeError(
                    "Current onyx state does not match the upstream "
                    "context of the cherami state. Cannot proceed."
                )
        except KeyError as k:
            raise ValueError(
                "Onyx versions hash not available in the message payload, "
                "cannot decipher upstream context."
            ) from k

        context.orange_box_version = payload["orange_box_version"]
        return context

    def should_run(self, context: PipelineContext) -> bool:
        """
        Determine whether the pipeline should run for the given context.

        Override this to implement decision logic for specific pathchar.

        When this returns False, the worker calls `on_skip()` instead of
        launching the pipeline.

        Default pathogen characteristic pipeline should_run logic.
        1) query onyx for all available analysis tables
            - if cannot reach onyx, raise RuntimeError.
        2) filter this to just pipeline analyses (matches on 'pipeline_name')
            - if there are none, exit True.
        3) Gather all the combinations of upstream context and pipeline
        versions from the analysis tables. Keep as tuple, make a set of these.
        4) Make the tuple of current upstream context and pipeline version and
        compare.
            - if current combination exists, return False.
            - if current combination does not exist, return True.

        Args:
            context (PipelineContext): pipeline context object. This should
            hold the sample id, job id, orange box version, current onyx
            versions hash and the message payload from upstream.

        Raises:
            RuntimeError: Onyx cannot be queried for the analysis tables.

        Returns:
            bool: true or false for should_run.
        """
        from onyx_analysis_helper import onyx_analysis_helper_functions as oa

        # 1) get all the analysis tables associated with the sample.

        analysis_tables: dict
        exitcode: int
        analysis_tables, exitcode = oa.get_analysis_records(
            sample_id=context.climb_id,
            server=context.server,
            fields=["methods", "pipeline_name", "pipeline_version"],
        )

        # If we cannot get to onyx, exit early
        if exitcode != 0:
            logger.error(
                "Cannot query Onyx for analyses for sample %s.",
                context.climb_id,
            )
            raise RuntimeError("Cannot query onyx - check logs for reasons.")

        # 2) Get the analysis tables associated with the pipeline:
        pipeline_analysis_tables = {
            aid: table
            for aid, table in analysis_tables.items()
            if table["pipeline_name"] == self.config.name
        }

        # If there are no analysis tables, just run:
        if not pipeline_analysis_tables:
            logger.debug(
                "Inbound sample %s has no analysis tables for pipeline %s.",
                context.climb_id,
                self.config.name,
            )
            return True

        # 3) Get a set of the (orange_box_version, onyx_version_hash,
        # pipeline_version) in all analysis tables
        upstream_contexts: set[tuple] = set()

        for table in analysis_tables.values():
            # add onyx versions hashes from analysis tables:
            onyx_versions_hash: str = table["methods"]["onyx_versions_hash"]

            # Get the orange box version from the analysis tables
            versions: list[dict] = table["methods"]["versions"]
            versions_dict: dict = {
                ver["name"]: ver["version"] for ver in versions
            }
            orange_box_version: str | None = versions_dict.get(
                "orange_box_version"
            )

            upstream_contexts.add(
                (
                    onyx_versions_hash,
                    orange_box_version,
                    table["pipeline_version"],
                )
            )

        # 4) Make the current tuple to compare:
        current_context: tuple[str | Any, str | Any, str | Any] = (
            context.onyx_versions_hash,
            context.orange_box_version,
            context.pipeline_version,
        )
        info: dict[str, str | Any] = dict(
            zip(
                ("hash", "orange box version", "pipeline version"),
                current_context,
                strict=True,
            )
        )
        if current_context in upstream_contexts:
            # do not rerun if previous upstream context matches current context
            logger.warning(
                "Sample %s has up-to-date analysis tables for pipeline %s, "
                "skipping.",
                context.climb_id,
                self.config.name,
            )
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "are up-to-date - current upstream context %s. "
                "Decision: not run.",
                context.climb_id,
                list(analysis_tables.keys()),
                info,
            )
            return False
        else:
            # This combination has not yet been run:
            logger.debug(
                "Inbound sample %s has analysis IDs %s. Analysis tables "
                "do not match current upstream context - %s. Decision: run.",
                context.climb_id,
                list(analysis_tables.keys()),
                info,
            )
            return True

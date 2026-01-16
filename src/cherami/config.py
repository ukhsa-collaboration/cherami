import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self


@dataclass(frozen=True)
class PipelineConfig:
    """Configuration for a pipeline."""

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
    ## k8 configs
    namespace: str
    container: str
    backoff_limit: int
    max_retries: int
    retry_timeout: int
    job_timeout: int

    @classmethod
    def from_dict(cls, raw_config: dict[str, Any]) -> Self:
        """Returns a PipelineConfig parsed from a raw config dictionary.

        Raises:
            ValueError: If a required field is missing.
        """
        try:
            return cls(
                name=str(raw_config["name"]),
                version=str(raw_config["version"]),
                path=str(raw_config["path"]),
                cpus=int(raw_config["cpus"]),
                mem=str(raw_config["mem"]),
                cpu_limit=int(raw_config["cpu_limit"]),
                mem_limit=str(raw_config["mem_limit"]),
                nf_config_path=Path(raw_config["nf_config_path"]),
                nf_profiles=list(raw_config["nf_profiles"]),
                nf_extra_args=list(raw_config["nf_extra_args"]),
                namespace=str(raw_config["namespace"]),
                container=str(raw_config["container"]),
                backoff_limit=int(raw_config["backoff_limit"]),
                max_retries=int(raw_config["max_retries"]),
                retry_timeout=int(raw_config["retry_timeout"]),
                job_timeout=int(raw_config["job_timeout"]),
            )
        except KeyError as error:
            raise ValueError(
                f"Pipeline missing required field: {error.args[0]}"
            ) from error


@dataclass(frozen=True)
class WorkerConfig:
    """Configuration for a worker."""

    listen_exchange: str
    listen_queue_suffix: str
    publish_queue_suffix: str | None
    publish_exchange: str | None
    varys_config_path: Path
    varys_log_path: Path
    config_path: Path
    config_hash: str

    @classmethod
    def from_dict(
        cls,
        raw_config: dict[str, Any],
        config_path: Path,
        config_hash: str,
    ) -> Self:
        """Returns a WorkerConfig parsed from a raw config dictionary.

        Raises:
            ValueError: If a required field is missing.
        """
        try:
            return cls(
                listen_exchange=str(raw_config["listen_exchange"]),
                listen_queue_suffix=str(raw_config["listen_queue_suffix"]),
                publish_queue_suffix=str(raw_config["publish_queue_suffix"])
                if raw_config["publish_queue_suffix"] is not None
                else None,
                publish_exchange=str(raw_config["publish_exchange"])
                if raw_config["publish_exchange"] is not None
                else None,
                varys_config_path=Path(raw_config["varys_config_path"]),
                varys_log_path=Path(raw_config["varys_log_path"]),
                config_path=config_path,
                config_hash=config_hash,
            )
        except KeyError as error:
            raise ValueError(
                f"Worker config missing required field: {error.args[0]}"
            ) from error


@dataclass(frozen=True)
class GlobalConfig:
    """Configuration for global settings."""

    work_dir: Path
    output_dir: Path

    @classmethod
    def from_dict(cls, raw_config: dict[str, Any]) -> Self:
        """Returns a GlobalConfig parsed from a raw config dictionary.

        Raises:
            ValueError: If a required field is missing.
        """
        try:
            return cls(
                work_dir=Path(raw_config["work_dir"]),
                output_dir=Path(raw_config["output_dir"]),
            )
        except KeyError as error:
            raise ValueError(
                f"Global config missing required field: {error.args[0]}"
            ) from error


@dataclass(frozen=True)
class CheramiConfig:
    """Top-level configuration storing all config sections."""

    global_config: GlobalConfig
    pipeline_config: PipelineConfig
    worker_config: WorkerConfig

    def pipeline_dirs(self) -> tuple[Path, Path]:
        """Returns pipeline-specific (e.g work_dir/output_dir) paths."""
        return (
            self.global_config.work_dir / self.pipeline_config.name,
            self.global_config.output_dir / self.pipeline_config.name,
        )


def load_config(config_path: Path) -> CheramiConfig:
    """Returns a CheramiConfig loaded from a JSON config file.

    Raises:
        OSError: If the config file cannot be read.
        json.JSONDecodeError: If the config file is not valid JSON.
        ValueError: If required config sections or fields are missing.
        Exception: If the pipeline module cannot be loaded.
    """
    with config_path.open("r") as f:
        raw_config = json.load(f)
    start_hash = hash_from_raw(raw_config)

    raw_global = raw_config.get("global")
    if raw_global is None:
        raise ValueError("Config missing 'global' section")
    global_config = GlobalConfig.from_dict(raw_global)

    raw_pipeline = raw_config.get("pipeline")
    if raw_pipeline is None:
        raise ValueError("Config missing 'pipeline' section")
    pipeline_config = PipelineConfig.from_dict(raw_pipeline)
    from cherami.pipelines import load_pipeline_module

    load_pipeline_module(pipeline_config.name)

    raw_worker = raw_config.get("worker")
    if raw_worker is None:
        raise ValueError("Config missing 'worker' section")
    worker_config = WorkerConfig.from_dict(
        raw_worker,
        config_path,
        start_hash,
    )

    return CheramiConfig(
        global_config=global_config,
        pipeline_config=pipeline_config,
        worker_config=worker_config,
    )


def hash_from_file(config_path: Path) -> str:
    """Returns a SHA-256 hash of a config file's JSON content.

    Utility function to hash directly from a file path.

    Raises:
        OSError: If the config file cannot be read.
        json.JSONDecodeError: If the config file is not valid JSON.
    """
    with config_path.open("r") as f:
        raw_config = json.load(f)
    return hash_from_raw(raw_config)


def hash_from_raw(raw_config: dict[str, Any]) -> str:
    """Returns a SHA-256 hash of a JSON content of a config.

    Used to ignore whitespace etc when hashing.
    """
    payload = json.dumps(
        raw_config,
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()

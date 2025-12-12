import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class PipelineConfig:
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
    def from_dict(cls, name: str, raw_config: dict[str, Any]) -> "PipelineConfig":
        try:
            return cls(
                name=name,
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
            raise ValueError(f"Pipeline '{name}' missing required field: {error.args[0]}") from error


@dataclass(frozen=True)
class WorkerConfig:
    worker_name: str
    pipeline_name: str
    listen_exchange: str
    listen_queue_suffix: str
    publish_queue_suffix: str | None
    publish_exchange: str | None
    varys_config_path: Path
    varys_log_path: Path

    @classmethod
    def from_dict(cls, name: str, raw: dict[str, Any]) -> "WorkerConfig":
        try:
            return cls(
                worker_name=name,
                pipeline_name=str(raw["pipeline_name"]),
                listen_exchange=str(raw["listen_exchange"]),
                listen_queue_suffix=str(raw["listen_queue_suffix"]),
                publish_queue_suffix=str(raw["publish_queue_suffix"])
                if raw["publish_queue_suffix"] is not None
                else None,
                publish_exchange=str(raw["publish_exchange"]) if raw["publish_exchange"] is not None else None,
                varys_config_path=Path(raw["varys_config_path"]),
                varys_log_path=Path(raw["varys_log_path"]),
            )
        except KeyError as error:
            raise ValueError(f"Worker '{name}' missing required field: {error.args[0]}") from error


@dataclass(frozen=True)
class GlobalConfig:
    work_dir: Path
    output_dir: Path

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "GlobalConfig":
        try:
            return cls(
                work_dir=Path(raw["work_dir"]),
                output_dir=Path(raw["output_dir"]),
            )
        except KeyError as error:
            raise ValueError(f"Global config missing required field: {error.args[0]}") from error


def load_raw_config_file(config_path: Path) -> dict[str, Any]:
    with config_path.open("r") as f:
        return json.load(f)


def parse_pipeline_config(name: str, raw_config: dict[str, Any]) -> PipelineConfig:
    pipelines = raw_config.get("pipelines") or {}
    if name not in pipelines:
        raise ValueError(f"Config missing pipeline '{name}'")
    raw_pipeline = pipelines[name]
    return PipelineConfig.from_dict(name, raw_pipeline)


def parse_worker_config(name: str, raw_config: dict[str, Any]) -> WorkerConfig:
    workers = raw_config.get("workers") or {}
    if name not in workers:
        raise ValueError(f"Config missing worker '{name}'")
    raw_worker = workers[name]
    return WorkerConfig.from_dict(name, raw_worker)


def parse_global_config(raw_config: dict[str, Any]) -> GlobalConfig:
    raw_global = raw_config.get("global")
    if raw_global is None:
        raise ValueError("Config missing 'global' section")
    return GlobalConfig.from_dict(raw_global)

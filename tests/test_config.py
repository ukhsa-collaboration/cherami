import json
from pathlib import Path

import pytest

from cherami.config import (
    CheramiConfig,
    GlobalConfig,
    PipelineConfig,
    WorkerConfig,
    load_config,
)


@pytest.fixture
def valid_global():
    return {
        "work_dir": "/idont/exist/work",
        "output_dir": "/idont/exist/output",
        "server": "fancyserver",
    }


@pytest.fixture
def valid_pipeline():
    return {
        "name": "test-pipeline",
        "version": "1.0.0",
        "path": "/idont/exist/pipeline",
        "cpus": 2,
        "mem": "4G",
        "cpu_limit": 4,
        "mem_limit": "8G",
        "nf_config_path": "/idont/exist/nf.config",
        "nf_profiles": ["docker", "test"],
        "nf_extra_args": ["--blah"],
        "namespace": "imafake-ns",
        "container": "nextflow/nextflow:latest",
        "backoff_limit": 3,
        "max_attempts": 2,
        "retry_timeout": 300,
        "job_timeout": 3600,
    }


@pytest.fixture
def valid_worker():
    return {
        "listen_exchange": "cherami-exchange",
        "listen_queue_suffix": "listen",
        "publish_queue_suffix": "publish",
        "publish_exchange": "next-exchange",
        "rerun_queue_suffix": "rerun_queue",
        "rerun_exchange": "rerun_exchange",
        "priority_queue_suffix": "priority_queue",
        "priority_exchange": "priority_exchange",
        "varys_config_path": "/idont/exist/varys.json",
        "varys_log_path": "/idont/exist/varys.log",
    }


def test_global_config_success(valid_global):
    config = GlobalConfig.from_dict(valid_global)  # ty:ignore[invalid-argument-type]

    assert config.work_dir == Path("/idont/exist/work")
    assert config.output_dir == Path("/idont/exist/output")
    assert config.server == "fancyserver"


def test_global_config_fail(valid_global):
    del valid_global["work_dir"]
    with pytest.raises(
        ValueError, match="Global config missing required field"
    ):
        GlobalConfig.from_dict(valid_global)


def test_pipeline_config_success(valid_pipeline):
    config = PipelineConfig.from_dict(valid_pipeline)

    assert config.name == "test-pipeline"
    assert config.cpus == 2
    assert config.nf_profiles == ["docker", "test"]
    assert config.nf_config_path == Path("/idont/exist/nf.config")


def test_pipeline_config_fail(valid_pipeline):
    del valid_pipeline["name"]
    with pytest.raises(ValueError, match="Pipeline missing required field"):
        PipelineConfig.from_dict(valid_pipeline)


def test_worker_config_success(valid_worker):
    config = WorkerConfig.from_dict(
        valid_worker,
        Path("/idont/exist/config.json"),
        "hash",
    )

    assert config.listen_exchange == "cherami-exchange"
    assert config.publish_queue_suffix == "publish"
    assert config.varys_config_path == Path("/idont/exist/varys.json")


@pytest.mark.parametrize("queue", ["publish", "rerun", "priority"])
def test_worker_config_allow_none_optionals(valid_worker, queue):
    """Any of publish, rerun or priority queue suffix and exchange can be
    None"""
    valid_worker[f"{queue}_queue_suffix"] = None
    valid_worker[f"{queue}_exchange"] = None

    config = WorkerConfig.from_dict(
        valid_worker,
        Path("/idont/exist/config.json"),
        "hash",
    )

    assert config.__dict__[f"{queue}_queue_suffix"] is None
    assert config.__dict__[f"{queue}_exchange"] is None


def test_worker_config_fail(valid_worker):
    del valid_worker["listen_exchange"]
    with pytest.raises(
        ValueError, match="Worker config missing required field"
    ):
        WorkerConfig.from_dict(
            valid_worker,
            Path("/idont/exist/config.json"),
            "hash",
        )


def test_cherami_config_pipeline_dirs(
    valid_global, valid_pipeline, valid_worker
):
    global_conf = GlobalConfig.from_dict(valid_global)
    pipeline_conf = PipelineConfig.from_dict(valid_pipeline)
    worker_conf = WorkerConfig.from_dict(
        valid_worker,
        Path("/idont/exist/config.json"),
        "hash",
    )
    cherami_conf = CheramiConfig(global_conf, pipeline_conf, worker_conf)
    work_dir, out_dir = cherami_conf.pipeline_dirs()

    assert work_dir == Path("/idont/exist/work/test-pipeline")
    assert out_dir == Path("/idont/exist/output/test-pipeline")


def test_load_config_valid(
    tmp_path, valid_global, valid_pipeline, valid_worker, mocker
):
    config_data = {
        "global": valid_global,
        "pipeline": valid_pipeline,
        "worker": valid_worker,
    }
    config_file = tmp_path / "config.json"
    with config_file.open("w") as f:
        json.dump(config_data, f)
    mock_load = mocker.patch("cherami.pipelines.load_pipeline_module")
    config = load_config(config_file)

    assert isinstance(config, CheramiConfig)
    assert config.global_config.work_dir == Path("/idont/exist/work")
    assert config.pipeline_config.name == "test-pipeline"
    mock_load.assert_called_once_with("test-pipeline")


def test_load_config_valid_but_incomplete(tmp_path, valid_global):
    config_data = {"global": valid_global}
    config_file = tmp_path / "incomplete_config.json"
    with config_file.open("w") as f:
        json.dump(config_data, f)

    with pytest.raises(ValueError, match="missing 'pipeline' section"):
        load_config(config_file)


def test_load_config_invalid_json(tmp_path):
    config_file = tmp_path / "bad.json"
    config_file.write_text('{"max_attempts": iminvalidjson}')

    with pytest.raises(ValueError, match="invalid JSON"):
        load_config(config_file)


@pytest.mark.parametrize("value", [0, -1])
def test_load_config_maxattempts_wrong_value(
    tmp_path, valid_global, valid_pipeline, valid_worker, mocker, value
):
    mocker.patch("cherami.pipelines.load_pipeline_module")
    valid_pipeline["max_attempts"] = value
    config_data = {
        "global": valid_global,
        "pipeline": valid_pipeline,
        "worker": valid_worker,
    }
    config_file = tmp_path / "config.json"
    with config_file.open("w") as f:
        json.dump(config_data, f)

    with pytest.raises(ValueError, match="max_attempts must be at least 1"):
        load_config(config_file)

from pathlib import Path

import pytest

from cherami.config import PipelineConfig
from cherami.pipelines.pipeline import Pipeline


class TestPipeline(Pipeline):
    def generate_samplesheet(self, samples, job_id, output_filepath, context):
        pass


@pytest.fixture
def pipeline_config():
    return PipelineConfig(
        name="test-pipeline",
        version="1.0.0",
        path="main.nf",
        cpus=2,
        mem="4G",
        cpu_limit=4,
        mem_limit="8G",
        nf_config_path=Path("/idont/exist/nf.config"),
        nf_profiles=["docker", "test"],
        nf_extra_args=["--blah"],
        namespace="imafake-ns",
        container="nextflow/nextflow:latest",
        backoff_limit=3,
        max_attempts=2,
        retry_timeout=300,
        job_timeout=3600,
    )


@pytest.fixture
def pipeline(pipeline_config, global_config):
    return TestPipeline(pipeline_config, global_config)


@pytest.fixture
def job_dirs():
    return {
        "working_dir": Path("/idont/exist/work"),
        "output_dir": Path("/idont/exist/output"),
        "nxf_work_dir": Path("/idont/exist/nxf_work"),
        "nxf_home_dir": Path("/idont/exist/nxf_home"),
        "nxf_log_file": Path("/idont/exist/nxf.log"),
        "samplesheet_path": Path("/idont/exist/samplesheet.csv"),
    }


def test_create_job_manifest(pipeline, pipeline_config, job_dirs, monkeypatch):
    monkeypatch.setenv("ONYX_TOKEN", "imaafaketoken")
    monkeypatch.setenv("ONYX_DOMAIN", "imaafakedomain")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "imaafakesecret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "imaafakekey")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "imaafake-endpoint")
    monkeypatch.setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "when_required")
    manifest = pipeline.create_job_manifest(
        job_id="JOB123",
        job_dirs=job_dirs,
    )
    container = manifest["spec"]["template"]["spec"]["containers"][0]
    command = container["args"][2]
    expected_command = (
        f"nextflow run main.nf -c {Path('/idont/exist/nf.config')} "
        f"-profile docker,test -r 1.0.0 --blah "
        f"--outdir {Path('/idont/exist/output')} --samplesheet "
        f"{Path('/idont/exist/samplesheet.csv')}"
    )

    assert manifest["metadata"]["name"] == f"{pipeline_config.name}-JOB123"
    assert manifest["metadata"]["namespace"] == pipeline_config.namespace
    assert manifest["spec"]["backoffLimit"] == pipeline_config.backoff_limit
    assert container["image"] == pipeline_config.container
    assert container["resources"]["requests"]["cpu"] == str(
        pipeline_config.cpus
    )
    assert container["resources"]["requests"]["memory"] == pipeline_config.mem
    assert container["resources"]["limits"]["cpu"] == str(
        pipeline_config.cpu_limit
    )
    assert (
        container["resources"]["limits"]["memory"] == pipeline_config.mem_limit
    )
    assert container["workingDir"] == str(job_dirs["working_dir"])
    assert command == expected_command


def test_create_job_manifest_missing_env_vars(pipeline, job_dirs, monkeypatch):
    for var in ["ONYX_TOKEN", "ONYX_DOMAIN", "AWS_ACCESS_KEY_ID"]:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(
        RuntimeError, match="Missing required environment variables"
    ):
        pipeline.create_job_manifest(
            job_id="JOB123",
            job_dirs=job_dirs,
        )

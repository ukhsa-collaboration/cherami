from pathlib import Path

import pytest

from cherami.config import PipelineConfig
from cherami.pipelines import Pipeline


class DummyPipeline(Pipeline):
    def __init__(
        self,
        config: PipelineConfig,
        proc_names: dict[str, list[int]] | None = None,
    ):
        super().__init__(config)
        self._proc_names = proc_names or {}

    @property
    def proc_names(self) -> dict[str, list[int]]:
        return self._proc_names

    def generate_samplesheet(self, samples, job_id, output_filepath):
        return


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
        max_retries=2,
        retry_timeout=300,
        job_timeout=3600,
    )


@pytest.fixture
def pipeline(pipeline_config):
    return DummyPipeline(config=pipeline_config)


@pytest.fixture
def pipeline_proc_names(pipeline_config):
    proc_names = {"NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)": [0, 5]}
    return DummyPipeline(config=pipeline_config, proc_names=proc_names)


@pytest.fixture
def trace_file(tmp_path, content):
    trace_path = tmp_path / "trace.tsv"
    trace_path.write_text(
        "task_id\thash\tnative_id\tname\tstatus\texit\tsubmit\tduration\trealtime\t%cpu\tpeak_rss\tpeak_vmem\trchar\twchar\n"
        + content,
    )
    return trace_path


@pytest.mark.parametrize(
    "content",
    [
        "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_should_pass(pipeline, trace_file):
    assert pipeline.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t1\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_should_fail(pipeline, trace_file):
    assert pipeline.evaluate_exit_status(trace_file) is False


@pytest.mark.parametrize(
    "content",
    [
        "2\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t5\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n3\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_proc_names_should_pass(
    pipeline_proc_names, trace_file
):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        "4\t83/cacaed\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\tNFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t1\t2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t19.5 MB\t4.6 MB\n",
    ],
)
def test_eval_exit_status_proc_names_should_fail(
    pipeline_proc_names, trace_file
):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False

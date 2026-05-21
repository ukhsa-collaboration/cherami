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
        (
            "task_id\thash\tnative_id\tname\tstatus\texit\tsubmit\tduration\t"
            "realtime\t%cpu\tpeak_rss\tpeak_vmem\trchar\twchar\n"
        )
        + content,
    )
    return trace_path


@pytest.fixture
def empty_trace_file(tmp_path):
    trace_path = tmp_path / "trace.tsv"
    trace_path.touch()
    return trace_path


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status_should_pass(pipeline, trace_file):
    assert pipeline.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t1\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status_should_fail(pipeline, trace_file, caplog):
    assert pipeline.evaluate_exit_status(trace_file) is False
    assert "ERROR" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "2\t80/cac7ef\tnf-80cac7edfcf0128514abf5f17718a8af-b277f\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t1\t"
            "2025-09-24 12:17:37.440\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t80/cac7eg\tnf-80cac7edfcf0128514abf5f17718a8af-b277g\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.441\t13.6s\t9s\t164.1%\t526.8 MB\7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "4\t80/cac7eh\tnf-80cac7edfcf0128514abf5f17718a8af-b277h\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE3_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.442\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__fail_then_complete_should_pass(
    pipeline, trace_file
):
    assert pipeline.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "2\t80/cac7ef\tnf-80cac7edfcf0128514abf5f17718a8af-b277f\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t1\t"
            "2025-09-24 12:17:37.440\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t80/cac7eg\tnf-80cac7edfcf0128514abf5f17718a8af-b277g\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.441\t13.6s\t9s\t164.1%\t526.8 MB\7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "4\t80/cac7eh\tnf-80cac7edfcf0128514abf5f17718a8af-b277h\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE3_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.442\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "5\t80/cac7ex\tnf-80cac7edfcf0128514abf5f17718a8af-b277x\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE4_PE)\tFAILED\t1\t"
            "2025-09-24 12:17:37.443\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t1"
            "9.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__one_fails(pipeline, trace_file, caplog):
    assert pipeline.evaluate_exit_status(trace_file) is False
    assert "ERROR" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        (
            "4\t80/cac7ef\tnf-80cac7edfcf0128514abf5f17718a8af-b277f\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t1\t"
            "2025-09-24 12:14:37.440\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "2\t80/cac7ef\tnf-80cac7edfcf0128514abf5f17718a8af-b277f\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t1\t"
            "2025-09-24 12:12:37.440\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "5\t80/cac7eh\tnf-80cac7edfcf0128514abf5f17718a8af-b277h\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE3_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.442\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:10:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "6\t80/cac7eg\tnf-80cac7edfcf0128514abf5f17718a8af-b277g\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:19:37.440\t13.6s\t9s\t164.1%\t526.8 MB\7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t80/cac7ef\tnf-80cac7edfcf0128514abf5f17718a8af-b277f\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t1\t"
            "2025-09-24 12:13:37.440\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__not_chronological(pipeline, trace_file):
    """
    Check that if the pipeline trace file is not chronological, any Complete
    for process will still returns True. Noting that this still assumes the
    last process will complete. It assumes Nextflow will not schedule a process
    that completes, then afterwards schedules the same process that fails.
    """
    assert pipeline.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        (
            "2\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t5\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status_proc_names_should_pass(
    pipeline_proc_names, trace_file
):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t82/cac9ec\tnf-82cac7edfcf0128514abf5f17718a8af-b277c\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tFAILED\t1\t"
            "2025-09-24 12:16:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "2\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t5\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:18:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status_proc_names_fails_then_completes(
    pipeline_proc_names, trace_file
):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is True


@pytest.mark.parametrize(
    "content",
    [
        (
            "4\t83/cacaed\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tFAILED\t1\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "5\t83/cacaee\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t2\t"
            "2025-09-24 12:18:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status_proc_names_completes_invalid_exitcode(
    pipeline_proc_names, trace_file, caplog
):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False
    assert "ERROR" in caplog.text


def test_eval_exit_status__entirely_empty_trace_file(
    pipeline_proc_names, empty_trace_file, caplog
):
    """Check that entirely empty file returns false and logs warning."""
    assert pipeline_proc_names.evaluate_exit_status(empty_trace_file) is False
    assert "ERROR" in caplog.text


@pytest.mark.parametrize(
    "content",
    ["\n"],
)
def test_eval_exit_status__empty_trace_file(
    pipeline_proc_names, trace_file, caplog
):
    """
    Check that file just with header and newline returns false and logs error.
    """
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False
    assert "ERROR" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        (
            "\n\t\t\t\t\t\t\t\t\t\t\t\t\t\n"
            "4\t83/cacaed\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tFAILED\t1\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "\n\t\t\t\t\t\t\t\t\t\t\t\t\t\n"
            "5\t83/cacaee\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t2\t"
            "2025-09-24 12:18:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__empty_rows_then_rows(
    pipeline_proc_names, trace_file, caplog
):
    """
    Check that a csv with an empty line (with columns though) is handled.
    """
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False
    assert "ERROR" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        (
            "4\t83/cacaed\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tFAILED\tnot a real exitcode\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__bad_exitcodes(
    pipeline_proc_names, trace_file, caplog
):
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False
    # should get warning about non-int exitcode and error about failing exitcode.
    assert "WARNING" in caplog.text
    assert "got not a real exitcode" in caplog.text
    assert "ERROR" in caplog.text
    assert "failed with exit code" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t82/cac9ec\tnf-82cac7edfcf0128514abf5f17718a8af-b277c\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tFAILED\t1\t"
            "2025-09-24 12:16:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "2\t82/cac9ed\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t5\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t82/cac9ee\tnf-82cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:18:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "4\t83/cacfff\tnf-83cac7edfcf0128514abf5f17718a8af-bffff\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t-\t"
            "2025-09-24 12:20:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__fails_nonint_exitcode(
    pipeline_proc_names, trace_file, caplog
):
    """If one process fails with non-integer exitcode and doesn't complete at
    all, it should fail overall."""
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is False
    # should get warning about non-int exitcode and error about failing exitcode.
    assert "WARNING" in caplog.text
    assert "got -" in caplog.text
    assert "ERROR" in caplog.text
    assert "failed with exit code" in caplog.text


@pytest.mark.parametrize(
    "content",
    [
        (
            "1\t80/cac7ed\tnf-80cac7edfcf0128514abf5f17718a8af-b277e\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "2\t80/cac7ef\tnf-80cac7edfcf0128514abf5f17718a8af-b277f\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tFAILED\t1\t"
            "2025-09-24 12:18:37.440\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "3\t80/cac7eg\tnf-80cac7edfcf0128514abf5f17718a8af-b277g\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE2_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:19:37.441\t13.6s\t9s\t164.1%\t526.8 MB\7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "4\t80/cac7eh\tnf-80cac7edfcf0128514abf5f17718a8af-b277h\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE3_PE)\tCOMPLETED\t0\t"
            "2025-09-24 12:20:37.442\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
            "5\t80/cac7ex\tnf-80cac7edfcf0128514abf5f17718a8af-b277x\t"
            "NFCORE_DEMO:DEMO:FASTQC (SAMPLE4_PE)\tFAILED\t-\t"
            "2025-09-24 12:20:37.443\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
            "19.5 MB\t4.6 MB\n"
        ),
    ],
)
def test_eval_exit_status__one_failswith_noninteger_exitcode(
    pipeline, trace_file, caplog
):
    assert pipeline.evaluate_exit_status(trace_file) is False
    # should get warning about non-int exitcode and error about non-zero exitcode.
    assert "WARNING" in caplog.text
    assert "got -" in caplog.text
    assert "ERROR" in caplog.text
    assert "failed with exit code" in caplog.text

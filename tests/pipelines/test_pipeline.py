import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from cherami.config import GlobalConfig, PipelineConfig
from cherami.pipelines import Pipeline
from cherami.pipelines.pipeline import (
    PathCharPipeline,
    PipelineContext,
    get_context_from_record,
)


class DummyPipeline(Pipeline):
    def __init__(
        self,
        config: PipelineConfig,
        global_config: GlobalConfig,
        proc_names: dict[str, list[int]] | None = None,
    ):
        super().__init__(config, global_config)
        self._proc_names = proc_names or {}

    @property
    def proc_names(self) -> dict[str, list[int]]:
        return self._proc_names

    def generate_samplesheet(self, samples, job_id, output_filepath, context):
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
        max_attempts=2,
        retry_timeout=300,
        job_timeout=3600,
    )


@pytest.fixture
def pipeline(pipeline_config, global_config):
    return DummyPipeline(config=pipeline_config, global_config=global_config)


@pytest.fixture
def pipeline_proc_names(pipeline_config, global_config):
    proc_names = {"NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)": [0, 5]}
    return DummyPipeline(
        config=pipeline_config,
        global_config=global_config,
        proc_names=proc_names,
    )


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
def test_eval_exit_status__ignores_nonint_exitcode(
    pipeline_proc_names, trace_file
):
    """
    One process fails with non-integer exitcode BUT that process is
    not defined in proc_names, so it will pass. Only process SAMPLE1_PE is
    checked.
    """
    assert pipeline_proc_names.evaluate_exit_status(trace_file) is True


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


@pytest.fixture
def missing_col_trace_file(tmp_path):
    trace_path = tmp_path / "missing_trace.tsv"
    trace_path.write_text(
        "task_id\thash\tnative_id\tname\tstatus\tsubmit\tduration\t"
        "realtime\t%cpu\tpeak_rss\tpeak_vmem\trchar\twchar\n"
        "4\t83/cacaed\tnf-83cac7edfcf0128514abf5f17718a8af-b277e\t"
        "NFCORE_DEMO:DEMO:FASTQC (SAMPLE1_PE)\tFAILED\t"
        "2025-09-24 12:17:37.439\t13.6s\t9s\t164.1%\t526.8 MB\t7.1 GB\t"
        "19.5 MB\t4.6 MB\n"
    )
    return trace_path


def test_eval_exit_status__missing_col_in_file(
    pipeline, missing_col_trace_file, caplog
):
    assert pipeline.evaluate_exit_status(missing_col_trace_file) is False
    assert "ERROR" in caplog.text
    assert "Expected to find column 'exit'" in caplog.text


def test_pipeline_context(mock_analysis_1):
    context = PipelineContext(
        mock_analysis_1.payload, server="server", pipeline_version="1.2.3"
    )
    assert context.climb_id == "ID-123456"
    # The onyx_versions_hash doesn't exist until the attr is assigned
    assert not context.onyx_versions_hash


@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get",
)
def test_pipeline_context_get_upstream_context_hash(
    mock_onyx, mock_analysis_1
):
    """
    This is a spurious test because it really tests that the PipelineContext object
    is populated from a query. The query result is patched in - this function would
    fail in production if the query results changed.
    """
    mock_onyx.return_value = mock_analysis_1.onyx_record
    context = context = PipelineContext(
        mock_analysis_1.payload, server="server", pipeline_version="1.2.3"
    )
    actual_hash = context.get_upstream_context_hash()
    assert actual_hash == mock_analysis_1.onyx_versions_hash


class DummyPathCharPipeline(PathCharPipeline):
    def generate_samplesheet(self, samples, job_id, output_filepath, context):
        return


@pytest.fixture
def path_char_pipeline(pipeline_config, global_config):
    return DummyPathCharPipeline(
        config=pipeline_config, global_config=global_config
    )


@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get"
)
def test_pathchar_pipeline_build_context(
    mock_onyx, path_char_pipeline, mock_analysis_1
):
    """Test that pathchar build_context adds orange box version and hash from
    payload to context object."""
    mock_onyx.return_value = mock_analysis_1.onyx_record
    mock_analysis_1.payload.update(
        {
            "orange_box_version": "1.2.3",
            "onyx_versions_hash": mock_analysis_1.onyx_versions_hash,
        }
    )

    path_char_context = path_char_pipeline.build_context(
        mock_analysis_1.payload
    )
    assert (
        path_char_context.onyx_versions_hash
        == mock_analysis_1.onyx_versions_hash
    )
    assert path_char_context.orange_box_version == "1.2.3"


@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get"
)
def test_pathchar_pipeline_build_context_mismatch_hash(
    mock_onyx, path_char_pipeline, mock_analysis_1
):
    """Test that pathchar build_context exits if hashes don't match."""
    mock_onyx.return_value = mock_analysis_1.onyx_record
    mock_analysis_1.payload.update(
        {
            "orange_box_version": "1.2.3",
            "onyx_versions_hash": "123456abcdef",
        }
    )
    with pytest.raises(RuntimeError) as r:
        path_char_pipeline.build_context(mock_analysis_1.payload)
    assert "Current onyx state does not match the upstream" in str(r.value)


@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get",
)
def test_pathchar_pipeline_build_context_incomplete_payload(
    mock_onyx, path_char_pipeline, mock_analysis_1, caplog
):
    """Check pathchar build context raises valueerror if incomplete payload."""
    caplog.set_level(logging.DEBUG)
    mock_onyx.return_value = mock_analysis_1.onyx_record
    payload = {
        "climb_id": "C123ABC",
        "match_uuid": "JOB123",
        "test": "test",
    }
    # This was changed to prevent crashing during testing where old messaged didn't have context
    # with pytest.raises(
    #     ValueError, match="not available in the message payload"
    # ):
    #     path_char_pipeline.build_context(payload)
    path_char_pipeline.build_context(payload)
    assert "not available in the message payload" in caplog.text


def test_pathchar_should_run_true(
    path_char_pipeline, mock_analysis_empty, caplog
):
    """Check that if there are no analysis records, should_run returns true"""
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = (mock_analysis_empty.analysis_tables, 0)
        context = PipelineContext(
            mock_analysis_empty.payload, "server", "2.2.2"
        )
        assert path_char_pipeline.should_run(context)
        assert "has no analysis tables for pipeline" in caplog.text


def test_pathchar_should_run_false(
    path_char_pipeline, mock_analysis_1, caplog
):
    """
    Check should_run returns false when analysis table present with matching
    upstream context and pipeline version.
    """
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = mock_analysis_1.analysis_tables, 0
        assert not path_char_pipeline.should_run(mock_analysis_1.context)
        assert "Decision: not run." in caplog.text


def test_pathchar_should_run_new_orange_box(
    mock_analysis_1, path_char_pipeline, caplog
):
    """Check should_run runs if new orange box version."""
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = mock_analysis_1.analysis_tables, 0
        # update just the orange box version in the mocked context object:
        mock_analysis_1.context.orange_box_version = "2.0.0"
        assert path_char_pipeline.should_run(mock_analysis_1.context)
        assert "Decision: run." in caplog.text


def test_pathchar_should_run_new_onyx_hash(
    mock_analysis_1, path_char_pipeline, caplog
):
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = mock_analysis_1.analysis_tables, 0
        # overwrite in mock test object - this would be the new hash calculated
        mock_analysis_1.context.onyx_versions_hash = "abc123"

        assert path_char_pipeline.should_run(mock_analysis_1.context)
        assert "Decision: run." in caplog.text


def test_pathchar_should_run_new_pathchar(
    mock_analysis_1, path_char_pipeline, caplog
):
    """Check should_run will run if pathchar version bumped."""
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = mock_analysis_1.analysis_tables, 0
        # Change the pipeline version in the mocked context object
        mock_analysis_1.context.pipeline_version = "2.0.0"
        assert path_char_pipeline.should_run(mock_analysis_1.context)
        assert "Decision: run." in caplog.text


def test_pathchar_should_run_many_tables(
    mock_multiple_analyses, path_char_pipeline, caplog
):
    """
    Should_run - sample has matching analysis table for pipeline with old
    context and an analysis table with the next context but not from the
    pipeline.
    """
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = mock_multiple_analyses.analysis_tables, 0
        assert path_char_pipeline.should_run(mock_multiple_analyses.context)
        assert "Decision: run." in caplog.text


def test_get_context_from_record(mock_analysis_1):
    onyx_hash, orange_box_version = get_context_from_record(
        mock_analysis_1.analysis_tables["AID-12345678"], "AID-12345678"
    )
    assert onyx_hash == mock_analysis_1.onyx_versions_hash
    assert orange_box_version == mock_analysis_1.orange_box_version


def test_get_context_from_record_keyerror(mock_analysis_1, caplog):
    """If onyx_hash or orange_box_version doesn't exist in the record then
    get keyerror and log."""
    caplog.set_level(logging.WARNING)
    record_no_context = mock_analysis_1.analysis_tables["AID-12345678"]
    record_no_context["methods"] = {}

    with pytest.raises(KeyError):
        assert get_context_from_record(record_no_context, "AID-123456")
    assert "Analysis record for ID AID-123456 does not have key" in caplog.text


def test_get_context_from_record_no_methods(mock_analysis_1, caplog):
    """If methods is missing, get key error but not logged."""
    record_no_context = mock_analysis_1.analysis_tables["AID-12345678"]
    record_no_context.pop("methods")

    with pytest.raises(KeyError):
        get_context_from_record(record_no_context, "AID-123456")

import csv
import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from cherami.config import PipelineConfig
from cherami.pipelines.orange_box import OrangeBoxPipeline

PipelineConfig.version = "1.0.0"


@pytest.fixture
def orangebox_config():
    return PipelineConfig(
        name="test-orange-box",
        version="1.2.3",
        path="orange_box/main.nf",
        cpus=1,
        mem="1G",
        cpu_limit=2,
        mem_limit="2G",
        nf_config_path=Path("/idont/exist/nf.config"),
        nf_profiles=["test"],
        nf_extra_args=["--blah"],
        namespace="imafake-ns",
        container="nextflow/nextflow:latest",
        backoff_limit=0,
        max_attempts=2,
        retry_timeout=300,
        job_timeout=3600,
    )


@pytest.fixture
def orange_box_pipeline(orangebox_config, global_config):
    return OrangeBoxPipeline(orangebox_config, global_config)


class MockMessage:
    def __init__(self, body):
        self.body = body


@pytest.fixture
def message():
    payload = {"climb_id": "C123ABC", "match_uuid": "JOB123", "test": "test"}
    test_message = MockMessage(body=json.dumps(payload))
    return test_message


def test_generate_samplesheet_success(
    tmp_path, orange_box_pipeline, test_context
):
    samples = ["CLIMB-001", "CLIMB-002"]
    job_id = "JOB123"
    output_filepath = tmp_path / "samplesheet.csv"

    orange_box_pipeline.generate_samplesheet(
        samples, job_id, output_filepath, test_context
    )
    with output_filepath.open("r") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    assert output_filepath.exists()
    assert fieldnames == ["climb_id"]
    assert len(rows) == 2
    assert rows[0]["climb_id"] == "CLIMB-001"
    assert rows[1]["climb_id"] == "CLIMB-002"


def test_generate_samplesheet_empty_fail(
    tmp_path, orange_box_pipeline, test_context
):
    samples = []
    job_id = "JOB123"
    output_filepath = tmp_path / "samplesheet.csv"
    with pytest.raises(ValueError, match="samplesheet_generation_no_records"):
        orange_box_pipeline.generate_samplesheet(
            samples, job_id, output_filepath, context=test_context
        )


@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get",
)
def test_orange_box_build_context(
    mock_onyx, orange_box_pipeline, mock_analysis_1
):
    """
    Tests that the build_context function creates and populates the hash
    from the onyx query (patched result).
    """
    mock_onyx.return_value = mock_analysis_1.onyx_record
    context = orange_box_pipeline.build_context(mock_analysis_1.payload)
    assert context.climb_id == "ID-123456"
    assert context.onyx_versions_hash == mock_analysis_1.onyx_versions_hash
    assert context.orange_box_version == mock_analysis_1.orange_box_version


def test_should_run_no_tables(
    orange_box_pipeline,
    mock_analysis_empty,
    caplog,
):
    """Should run - if not analyses are available."""
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = (mock_analysis_empty.analysis_records, 0)

        assert orange_box_pipeline.should_run(mock_analysis_empty.context)
        assert "has no analysis tables, running orange box" in caplog.text


def test_should_not_run_matching_upstream_and_ob_version(
    orange_box_pipeline,
    mock_analysis_1,
    caplog,
):
    """Should not run - has same version orange box and has same upstream context."""
    caplog.set_level(logging.DEBUG)
    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = (mock_analysis_1.analysis_tables, 0)
        assert not orange_box_pipeline.should_run(mock_analysis_1.context)
        # check warning:
        assert "has up-to-date analysis tables, skipping." in caplog.text
        # check debug:
        assert "Decision: not run." in caplog.text


def test_should_run_new_orange_box_version(
    orange_box_pipeline,
    mock_analysis_old_ob,
    caplog,
):
    """Should run - has same upstream context BUT old orange box version."""
    caplog.set_level(logging.DEBUG)

    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = (mock_analysis_old_ob.analysis_tables, 0)
        orange_box_pipeline.should_run(mock_analysis_old_ob.context)
        assert orange_box_pipeline.should_run(mock_analysis_old_ob.context)
        assert "Decision: run." in caplog.text


def test_should_run_different_upstream(
    orange_box_pipeline,
    mock_analysis_2,
    caplog,
):
    """Should run - has same orange box version BUT different upstream context."""
    caplog.set_level(logging.DEBUG)

    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = (mock_analysis_2.analysis_tables, 0)
        assert orange_box_pipeline.should_run(mock_analysis_2.context)
        assert "Decision: run." in caplog.text


def test_should_run_multiple_analyses_one_match(
    orange_box_pipeline,
    mock_multiple_analyses,
    caplog,
):
    """Should not run - has multiple analysis tables but same orange box version and same
    upstream context."""
    caplog.set_level(logging.DEBUG)

    with patch(
        "onyx_analysis_helper.onyx_analysis_helper_functions.get_analysis_records",
    ) as mock_analysis:
        mock_analysis.return_value = (
            mock_multiple_analyses.analysis_tables,
            0,
        )
        assert not orange_box_pipeline.should_run(
            mock_multiple_analyses.context
        )
        assert "Decision: not run." in caplog.text

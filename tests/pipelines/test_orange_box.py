import csv

import pytest

from cherami.pipelines.orange_box import OrangeBoxPipeline


@pytest.fixture
def mock_config(mocker):
    return mocker.Mock()


@pytest.fixture
def orange_box_pipeline(mock_config):
    return OrangeBoxPipeline(mock_config)


def test_generate_samplesheet_success(tmp_path, orange_box_pipeline):
    samples = ["CLIMB-001", "CLIMB-002"]
    job_id = "JOB123"
    output_filepath = tmp_path / "samplesheet.csv"
    orange_box_pipeline.generate_samplesheet(samples, job_id, output_filepath)
    with output_filepath.open("r") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    assert output_filepath.exists()
    assert fieldnames == ["climb_id"]
    assert len(rows) == 2
    assert rows[0]["climb_id"] == "CLIMB-001"
    assert rows[1]["climb_id"] == "CLIMB-002"


def test_generate_samplesheet_empty_fail(tmp_path, orange_box_pipeline):
    samples = []
    job_id = "JOB123"
    output_filepath = tmp_path / "samplesheet.csv"
    with pytest.raises(ValueError, match="samplesheet_generation_no_records"):
        orange_box_pipeline.generate_samplesheet(
            samples, job_id, output_filepath
        )

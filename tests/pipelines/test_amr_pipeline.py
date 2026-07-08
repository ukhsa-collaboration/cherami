import csv

import pytest

from cherami.pipelines.amr import AmrPipeline


@pytest.fixture
def mock_config(mocker):
    return mocker.Mock()


@pytest.fixture
def amr_pipeline(mock_config, global_config):
    pipeline = AmrPipeline(mock_config, global_config)
    return pipeline


@pytest.fixture
def mock_onyx_record():
    return {
        "human_filtered_reads_1": "/idont/exist/reads_1.fastq",
        "human_filtered_reads_2": "/idont/exist/reads_2.fastq",
        "taxon_reports": "/idont/exist/taxon_reports",
    }


def test_generate_samplesheet_success(
    tmp_path, monkeypatch, amr_pipeline, mock_onyx_record, mocker, test_context
):
    monkeypatch.setenv("ONYX_DOMAIN", "test.domain")
    monkeypatch.setenv("ONYX_TOKEN", "test_token")
    mock_onyx_client = mocker.patch("cherami.pipelines.amr.OnyxClient")
    mock_client = mocker.Mock()
    mock_client.get.return_value = mock_onyx_record
    mock_onyx_client.return_value.__enter__.return_value = mock_client
    destination = tmp_path / "CLIMB123_samplesheet.csv"
    amr_pipeline.generate_samplesheet(
        ["CLIMB123"], "JOB123", destination, test_context
    )
    with destination.open("r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert destination.exists()
    assert len(rows) == 1
    assert rows[0]["climb_id"] == "CLIMB123"
    assert rows[0]["human_filtered_reads_1"] == "/idont/exist/reads_1.fastq"
    assert rows[0]["taxon_reports"] == "/idont/exist/taxon_reports"


def test_generate_samplesheet_onyx_no_records(
    tmp_path, monkeypatch, amr_pipeline, mocker, test_context
):
    monkeypatch.setenv("ONYX_DOMAIN", "test.domain")
    monkeypatch.setenv("ONYX_TOKEN", "test_token")
    mock_onyx_client = mocker.patch("cherami.pipelines.amr.OnyxClient")
    mock_client = mocker.Mock()
    mock_client.filter.return_value = []
    mock_client.get.return_value = None
    mock_onyx_client.return_value.__enter__.return_value = mock_client
    destination = tmp_path / "CLIMB123_samplesheet.csv"
    with pytest.raises(ValueError, match="no_records_found"):
        amr_pipeline.generate_samplesheet(
            ["CLIMB123"], "JOB123", destination, test_context
        )


def test_generate_samplesheet_onyx_missing_fields(
    tmp_path, monkeypatch, amr_pipeline, mocker, test_context
):
    monkeypatch.setenv("ONYX_DOMAIN", "test.domain")
    monkeypatch.setenv("ONYX_TOKEN", "test_token")
    incomplete_record = {"human_filtered_reads_1": "foo"}
    mock_onyx_client = mocker.patch("cherami.pipelines.amr.OnyxClient")
    mock_client = mocker.Mock()
    mock_client.get.return_value = incomplete_record
    mock_onyx_client.return_value.__enter__.return_value = mock_client
    destination = tmp_path / "CLIMB123_samplesheet.csv"
    with pytest.raises(ValueError, match="missing_expected_data"):
        amr_pipeline.generate_samplesheet(
            ["CLIMB123"], "JOB123", destination, test_context
        )

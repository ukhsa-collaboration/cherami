import csv
from unittest.mock import Mock, patch

import pytest

from cherami.pipelines.amr import AmrPipeline


@pytest.fixture
def mock_config():
    return Mock()


@pytest.fixture
def amr_pipeline(mock_config):
    pipeline = AmrPipeline(mock_config)
    return pipeline


@pytest.fixture
def mock_onyx_record():
    return {
        "human_filtered_reads_1": "/i/dont/exist.fastq",
        "human_filtered_reads_2": "/i/dont/exist.fastq",
        "taxon_reports": "/i/dont/exist/taxon_reports",
    }


def test_generate_samplesheet_success(
    tmp_path, monkeypatch, amr_pipeline, mock_onyx_record
):
    monkeypatch.setenv("ONYX_DOMAIN", "test.domain")
    monkeypatch.setenv("ONYX_TOKEN", "test_token")

    with patch("cherami.pipelines.amr.OnyxClient") as mock_onyx_client:
        mock_client = Mock()
        mock_client.filter.return_value = [mock_onyx_record]
        mock_onyx_client.return_value.__enter__.return_value = mock_client

        destination = tmp_path / "CLIMB123_samplesheet.csv"
        amr_pipeline.generate_samplesheet(["CLIMB123"], "job_001", destination)

        assert destination.exists()

        with destination.open("r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["climb_id"] == "CLIMB123"
            assert rows[0]["human_filtered_reads_1"] == "/i/dont/exist.fastq"
            assert rows[0]["taxon_reports"] == "/i/dont/exist/taxon_reports"

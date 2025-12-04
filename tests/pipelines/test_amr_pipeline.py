import csv
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from cherami.pipelines.amr import AmrPipeline


@pytest.fixture
def mock_config():
    config = Mock()
    config.work_dir = Path(tempfile.mkdtemp())
    return config


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


@patch.dict(os.environ, {"ONYX_DOMAIN": "test.domain", "ONYX_TOKEN": "test_token"})
@patch("cherami.pipelines.amr.OnyxClient")
def test_generate_samplesheet_success(mock_onyx_client, amr_pipeline, mock_onyx_record):
    mock_client = Mock()
    mock_client.filter.return_value = [mock_onyx_record]
    mock_onyx_client.return_value.__enter__.return_value = mock_client

    result = amr_pipeline.generate_samplesheet(["CLIMB123"], "job_001")

    assert result.name == "amr_samplesheet_job_001.csv"
    assert result.exists()

    with result.open("r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["climb_id"] == "CLIMB123"
        assert rows[0]["human_filtered_reads_1"] == "/i/dont/exist.fastq"
        assert rows[0]["kraken_assignments"] == "/i/dont/exist/taxon_reports/CLIMB123_PlusPF.kraken_assignments.tsv"

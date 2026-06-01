import csv
from unittest.mock import patch

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


# set up some data to patch into onyx queries:

## Mock the main onyx query, from which get the record and versions.
MOCK_ONYX_RECORD_OLD: dict[str, str | dict] = {
    "climb_id": "ID-123456",
    "site": "test",
    "published_date": "2026-01-01",
    "data": {"datapoint1": 1, "datapoint2": 2, "datapoint3": 3},
    "classifier_version": "1.0.0",
    "classifier_db_date": "1970-01-01",
    "ncbi_taxonomy_date": "1970-01-01",
    "scylla_version": "1.0.0",
    "sylph_db_version": "1.0.0",
    "alignment_db_version": "1.0.0",
}

MOCK_ONYX_RECORD_OLD_NEW_CLASSIFIER: dict[str, str | dict] = {
    "climb_id": "ID-123456",
    "site": "test",
    "published_date": "2026-01-01",
    "data": {"datapoint1": 1, "datapoint2": 2, "datapoint3": 3},
    "classifier_version": "2.0.0",  # classifier has been bumped to v2
    "classifier_db_date": "2000-01-01",  # uses a new db too
    "ncbi_taxonomy_date": "1970-01-01",
    "scylla_version": "1.0.0",
    "sylph_db_version": "1.0.0",
    "alignment_db_version": "1.0.0",
}

# Mock the analysis records - these are returned by client.analyses
MOCK_ANALYSIS_RECORD = [
    {
        "published_date": "1970-01-01",
        "site": "test",
        "analysis_id": "AID-12345678",
        "analysis_date": "1970-01-01",
        "name": "test-analysis",
        "report": "",
        "outputs": "path/to/outputs/file.json",
    }
]

ANOTHER_MOCK_ANALYSIS_RECORD = [
    {
        "published_date": "1970-01-02",
        "site": "test-the-second",
        "analysis_id": "AID-89012345",
        "analysis_date": "1970-01-02",
        "name": "test-analysis",
        "report": "",
        "outputs": "path/to/file_2.json",
    }
]

## Mock the analysis table results - these are returned by client.get_analysis
MOCK_ANALYSIS_TABLE = {
    "name": "test-analysis",
    "description": "This is a test analysis",
    "analysis_date": "1970-01-01",
    "pipeline_name": "test-pipeline",
    "pipeline_url": "test-pipeline-url",
    "pipeline_version": "0.1.0",
    "result": "test result",
    "upstream_analyses": [],
    "report": "",
    "outputs": "path/to/outputs/file.json",
    "methods": {
        "versions": [
            {"name": "classifier_version", "version": "1.0.0"},  # onyx version
            {
                "name": "classifier_db_date",
                "version": "1970-01-01",
            },  # onyx version
            {
                "name": "ncbi_taxonomy_date",
                "version": "1970-01-01",
            },  # onyx version
            {"name": "scylla_version", "version": "1.0.0"},  # onyx version
            {"name": "sylph_db_version", "version": "1.0.0"},  # onyx version
            {
                "name": "alignment_db_version",
                "version": "1.0.0",
            },  # onyx version
            {"name": "module_dependency_db", "version": "2000-01-01"},
            {"name": "orange_box_version", "version": "1.2.3"},
        ],
        "onyx_versions_hash": "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614",
        "thresholds": {"limit": 10},
    },
    "result_metrics": {
        "Example result 1": 9,
        "Example result 2": "Fail",
        "Example result 3": 0.3,
    },
    "synthscape_records": ["ID-123456789"],
    "identifiers": [],
    "analysis_id": "AID-12345678",
}


# New pipeline and new orange box version, plus new classifier version, causes sample to 'pass' in module now.
ANOTHER_MOCK_ANALYSIS_TABLE = {
    "name": "test-analysis",
    "description": "This is another test analysis",
    "analysis_date": "1970-01-02",
    "pipeline_name": "test-pipeline",
    "pipeline_url": "test-pipeline-url",
    "pipeline_version": "0.2.0",  # pipeline version has been bumped
    "result": "another test result",
    "upstream_analyses": [],
    "report": "",
    "outputs": "path/to/file_2.json",
    "methods": {
        "versions": [
            {
                "name": "classifier_version",
                "version": "2.0.0",
            },  # New classifier! onyx version
            {
                "name": "classifier_db_date",
                "version": "2000-01-01",
            },  # New db! onyx version
            {
                "name": "ncbi_taxonomy_date",
                "version": "1970-01-01",
            },  # onyx version
            {"name": "scylla_version", "version": "1.0.0"},  # onyx version
            {"name": "sylph_db_version", "version": "1.0.0"},  # onyx version
            {
                "name": "alignment_db_version",
                "version": "1.0.0",
            },  # onyx version
            {"name": "module_dependency_db", "version": "2000-01-01"},
            {
                "name": "orange_box_version",
                "version": "1.2.3",
            },
        ],
        "onyx_versions_hash": "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614",
        "thresholds": {"limit": 10},
    },
    "result_metrics": {
        "Example result 1": 10,
        "Example result 2": "Pass",  # new classifier, now passes!
        "Example result 3": 0.5,
    },
    "synthscape_records": ["ID-123456789"],
    "identifiers": [],
    "analysis_id": "AID-89012345",
}


@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get_analysis",
    return_value=MOCK_ANALYSIS_TABLE.copy(),
)
@patch(
    target="onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.analyses",
    return_value=MOCK_ANALYSIS_RECORD.copy(),
)
@patch(
    "onyx_analysis_helper.onyx_analysis_helper_functions.OnyxClient.get",
    return_value=MOCK_ONYX_RECORD_OLD,
)
def test_should_run(
    onyx_versions_query,
    mocked_analyses,
    mocked_analysis_table,
    orange_box_pipeline,
):
    """Should not run - has same version orange box and has same upstream context."""
    from cherami.config import GlobalConfig, PipelineConfig

    GlobalConfig.server = "server"
    PipelineConfig.version = "1.2.3"

    assert not orange_box_pipeline.should_run("ID-123456")

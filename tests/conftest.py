import os
from dataclasses import dataclass

import pytest

from cherami.pipelines.pipeline import PipelineContext

os.environ["ONYX_DOMAIN"] = "Placeholder domain"
os.environ["ONYX_TOKEN"] = "Placeholder token"


class MockGlobalConfig:
    """Emulates the GlobalConfig object"""

    def __init__(self):
        self.work_dir = ("/path/to/dir",)
        self.output_dir = ("/path/to/output",)
        self.server = "server"


@pytest.fixture
def global_config():
    return MockGlobalConfig()


@dataclass
class MockedSample:
    sample_id: str
    analysis_ids: list
    onyx_record: dict
    onyx_versions_hash: str  # hash matchuing onyx_record above
    analysis_records: list[dict]
    analysis_tables: dict
    orange_box_version: str  # version of currently running ob
    payload: dict
    context: PipelineContext


class TestContext(PipelineContext):
    def __init__(self, payload, server, pipeline_version, onyx_hash):
        super().__init__(payload, server, pipeline_version)
        self.onyx_versions_hash = onyx_hash
        self.orange_box_version = "1.2.3"


@pytest.fixture
def test_context():
    payload = {
        "climb_id": "ID-123456",
        "match_uuid": "ABC123",
        "test": "test2",
    }
    context = TestContext(payload, "server", "1.0.0", "")
    return context


ONYX_RECORD = {
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
    "human_filtered_reads_1": "/path/to/forward/fastq",
    "human_filtered_reads_2": "",
    "taxon_reports": "path/to/taxon_reports/",
}
"""Mocked onyx client.get response using old onyx format."""

ONYX_HASH = "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614"
"""hash of the onyx versions in ONYX_RECORD."""

ANALYSIS_RECORD = [
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
"""an analysis record that returns one analysis table"""


ANALYSIS_TABLE = {
    "name": "test-analysis",
    "description": "This is a test analysis",
    "analysis_date": "1970-01-01",
    "pipeline_name": "test-pipeline",
    "pipeline_url": "test-pipeline-url",
    "pipeline_version": "1.0.0",
    "result": "test result",
    "upstream_analyses": [],
    "report": "",
    "outputs": "path/to/outputs/file.json",
    "methods": {
        "versions": [
            {
                "name": "classifier_version",
                "version": "1.0.0",
            },  # onyx version
            {
                "name": "classifier_db_date",
                "version": "1970-01-01",
            },  # onyx version
            {
                "name": "ncbi_taxonomy_date",
                "version": "1970-01-01",
            },  # onyx version
            {"name": "scylla_version", "version": "1.0.0"},  # onyx version
            {
                "name": "sylph_db_version",
                "version": "1.0.0",
            },  # onyx version
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
    "synthscape_records": ["ID-123456"],
    "identifiers": [],
    "analysis_id": "AID-12345678",
}
"""Analysis table associated with the analysis record."""

PAYLOAD: dict[str, str] = {
    "climb_id": "ID-123456",
    "match_uuid": "ABC123",
    "test": "test2",
}


@pytest.fixture
def mock_analysis_1():
    return MockedSample(
        sample_id="ID-123456",
        analysis_ids=["AID-12345678"],
        onyx_record=ONYX_RECORD,
        onyx_versions_hash=ONYX_HASH,
        analysis_records=ANALYSIS_RECORD,
        analysis_tables={
            "AID-12345678": {
                "pipeline_name": ANALYSIS_TABLE["pipeline_name"],
                "pipeline_version": ANALYSIS_TABLE["pipeline_version"],
                "methods": ANALYSIS_TABLE["methods"],
            }
        },
        orange_box_version="1.2.3",
        payload=PAYLOAD,
        context=TestContext(
            PAYLOAD,
            "server",
            ANALYSIS_TABLE["pipeline_version"],
            ONYX_HASH,
        ),
    )


@pytest.fixture
def mock_analysis_old_ob():
    """Older version of the orange box"""
    return MockedSample(
        sample_id="ID-123456",
        analysis_ids=["AID-12345678"],
        onyx_record=ONYX_RECORD,
        onyx_versions_hash=ONYX_HASH,
        analysis_records=ANALYSIS_RECORD,
        analysis_tables={
            "AID-12345678": {
                "pipeline_name": ANALYSIS_TABLE["pipeline_name"],
                "pipeline_version": "0.9.9",
                "methods": {
                    "versions": [
                        {
                            "name": "classifier_version",
                            "version": "1.0.0",
                        },  # onyx version
                        {
                            "name": "classifier_db_date",
                            "version": "1970-01-01",
                        },  # onyx version
                        {
                            "name": "ncbi_taxonomy_date",
                            "version": "1970-01-01",
                        },  # onyx version
                        {
                            "name": "scylla_version",
                            "version": "1.0.0",
                        },  # onyx version
                        {
                            "name": "sylph_db_version",
                            "version": "1.0.0",
                        },  # onyx version
                        {
                            "name": "alignment_db_version",
                            "version": "1.0.0",
                        },  # onyx version
                        {
                            "name": "module_dependency_db",
                            "version": "2000-01-01",
                        },
                        {"name": "orange_box_version", "version": "0.9.9"},
                    ],
                    "onyx_versions_hash": "e0c8c12a02fa86494059858c41af311d94c086a286bf4c62d53c21261e90f614",
                    "thresholds": {"limit": 10},
                },
            }
        },
        orange_box_version="1.2.3",
        payload=PAYLOAD,
        context=TestContext(
            PAYLOAD,
            "server",
            ANALYSIS_TABLE["pipeline_version"],
            ONYX_HASH,
        ),
    )


ONYX_RECORD_2 = {
    "climb_id": "ID-567890",
    "site": "test",
    "published_date": "2026-01-01",
    "data": {"datapoint1": 1, "datapoint2": 2, "datapoint3": 3},
    "classifier_version": "2.0.0",
    "classifier_db_date": "2000-01-01",
    "ncbi_taxonomy_date": "1970-01-01",
    "scylla_version": "1.0.0",
    "sylph_db_version": "1.0.0",
    "alignment_db_version": "1.0.0",
}
"""Mocked onyx client.get response using old onyx format but has newer
classifier version."""


ANALYSIS_RECORD_2 = [
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
"""A different analysis table"""

ANALYSIS_TABLE_2 = {
    "name": "test-analysis",
    "description": "This is another test analysis but from orange box",
    "analysis_date": "1970-01-02",
    "pipeline_name": "orange_box_module",
    "pipeline_url": "test-pipeline-url",
    "pipeline_version": "2.0.0",
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
        "onyx_versions_hash": "81929bfff563946f4e5643fe6e3441835d964e48f8476c67723cfec714550be2",
        "thresholds": {"limit": 10},
    },
    "result_metrics": {
        "Example result 1": 10,
        "Example result 2": "Pass",  # new classifier, now passes!
        "Example result 3": 0.5,
    },
    "synthscape_records": ["ID-123456"],
    "identifiers": [],
    "analysis_id": "AID-89012345",
}
"""Scylla has updated, and this sample now passes the orange box module. """


ONYX_HASH_2 = (
    "81929bfff563946f4e5643fe6e3441835d964e48f8476c67723cfec714550be2"
)
"""Onyx hash of analysis_table 2, with updated versions (Scylla has updated)"""


@pytest.fixture
def mock_multiple_analyses():
    return MockedSample(
        sample_id="ID-123456",
        analysis_ids=["AID-12345678", "AID-89012345"],
        onyx_record=ONYX_RECORD,
        onyx_versions_hash=ONYX_HASH_2,
        analysis_records=ANALYSIS_RECORD + ANALYSIS_RECORD_2,
        analysis_tables={
            "AID-12345678": {
                "pipeline_name": "test-pipeline",
                "pipeline_version": "1.0.0",
                "methods": ANALYSIS_TABLE["methods"],
            },
            "AID-89012345": {
                "pipeline_name": "orange_box_module",
                "pipeline_version": "2.0.0",
                "methods": ANALYSIS_TABLE_2["methods"],
            },
        },
        orange_box_version="1.2.3",
        payload=PAYLOAD,
        context=TestContext(
            PAYLOAD,
            "server",
            "1.0.0",
            onyx_hash=ONYX_HASH_2,
        ),
    )


@pytest.fixture
def mock_analysis_empty():
    return MockedSample(
        sample_id="ID-000000",
        analysis_ids=[],
        onyx_record={
            "climb_id": "ID-000000",
            "site": "test",
            "published_date": "2000-01-01",
            "data": {"datapoint1": 1, "datapoint2": 2, "datapoint3": 3},
            "classifier_version": "1.0.0",
            "classifier_db_date": "1970-01-01",
            "ncbi_taxonomy_date": "1970-01-01",
            "scylla_version": "1.0.0",
            "sylph_db_version": "1.0.0",
            "alignment_db_version": "1.0.0",
        },
        onyx_versions_hash=ONYX_HASH,  # same hash as same versions
        analysis_records=[],
        analysis_tables={},
        orange_box_version="1.2.3",
        payload={
            "climb_id": "ID-000000",
            "match_uuid": "XXX000",
            "test": "test2",
        },
        context=TestContext(
            payload={
                "climb_id": "ID-000000",
                "match_uuid": "XXX000",
                "test": "test2",
            },
            server="server",
            pipeline_version="1.0.0",
            onyx_hash=ONYX_HASH,
        ),
    )


@pytest.fixture
def mock_analysis_2():
    """Analysis tables have old upstream, hash is new"""
    payload = {
        "climb_id": "ID-567800",
        "match_uuid": "456DEF",
        "test": "test2",
    }
    return MockedSample(
        sample_id="ID-567890",
        analysis_ids=["AID-12345678"],
        onyx_record=ONYX_RECORD_2,
        onyx_versions_hash=ONYX_HASH_2,  # same hash as onyx versions
        analysis_records=ANALYSIS_RECORD,
        analysis_tables={
            "AID-12345678": {
                "pipeline_name": ANALYSIS_TABLE["pipeline_name"],
                "pipeline_version": ANALYSIS_TABLE["pipeline_version"],
                "methods": ANALYSIS_TABLE["methods"],
            }
        },
        orange_box_version="1.2.3",
        payload=payload,
        context=TestContext(
            payload=payload,
            server="server",
            pipeline_version="1.0.0",
            onyx_hash=ONYX_HASH_2,
        ),
    )

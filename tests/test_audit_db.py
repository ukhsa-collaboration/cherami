import sqlite3

import pytest

from cherami.audit_db import AuditDB
from cherami.pipelines.worker import PipelineResult


@pytest.fixture
def audit_db(tmp_path):
    db_path = tmp_path / "audit.db"
    return AuditDB(db_path)


@pytest.fixture
def sample_result():
    return PipelineResult(
        climb_id="C123ABC",
        job_uuid="JOB123",
        pipeline_name="amr-pipeline",
        status="SUCCESS",
        error_message=None,
        attempt=1,
        max_attempts=3,
        start_time="1000.0",
        end_time="1060.0",
        duration=60.0,
    )


def test_init_db_creates_table(audit_db):
    with sqlite3.connect(audit_db.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM audit_log")
        columns = {column[0] for column in cursor.description}
        expected_columns = {
            "id",
            "climb_id",
            "job_uuid",
            "pipeline_name",
            "audit_timestamp",
            "status",
            "error_message",
            "attempt",
            "max_attempts",
            "start_time",
            "end_time",
            "duration",
        }

        assert expected_columns.issubset(columns)


def test_add_record_success(audit_db, sample_result):
    audit_db.add_record(sample_result)
    with sqlite3.connect(audit_db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM audit_log WHERE climb_id = ?",
            (sample_result.climb_id,),
        )
        row = cursor.fetchone()

        assert row is not None
        assert row["climb_id"] == sample_result.climb_id
        assert row["job_uuid"] == sample_result.job_uuid
        assert row["pipeline_name"] == sample_result.pipeline_name
        assert row["status"] == sample_result.status
        assert row["attempt"] == sample_result.attempt
        assert row["duration"] == sample_result.duration
        assert row["audit_timestamp"] is not None


def test_add_record_skipped(audit_db):
    result = PipelineResult(
        climb_id="C123ABC",
        job_uuid="JOB123",
        pipeline_name="test-pipeline",
        status="SKIPPED",
    )
    audit_db.add_record(result)
    with sqlite3.connect(audit_db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM audit_log WHERE climb_id = ?", ("C123ABC",)
        )
        row = cursor.fetchone()

        assert row is not None
        assert row["status"] == "SKIPPED"
        assert row["error_message"] is None
        assert row["duration"] is None

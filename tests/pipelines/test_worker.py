import datetime as dt
import json
from pathlib import Path

import pytest

from cherami.config import WorkerConfig
from cherami.pipelines.pipeline import Pipeline
from cherami.pipelines.worker import Worker


@pytest.fixture
def mock_worker_config():
    return WorkerConfig(
        listen_exchange="test-exchange",
        listen_queue_suffix="queue",
        publish_queue_suffix="test",
        publish_exchange="out-exchange",
        varys_config_path=Path("/idont/exist/varys.conf"),
        varys_log_path=Path("/idont/exist/varys.log"),
        config_path=Path("/idont/exist/config.json"),
        config_hash="hash",
    )


@pytest.fixture
def mock_pipeline(mocker):
    pipeline = mocker.Mock(spec=Pipeline)
    pipeline.config = mocker.Mock()
    pipeline.config.name = "test-pipeline"
    return pipeline


@pytest.fixture
def worker(mock_worker_config, mock_pipeline, tmp_path):
    return Worker(
        worker_config=mock_worker_config,
        pipeline=mock_pipeline,
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
        audit_db_path=tmp_path / "audit.db",
    )


class MockMessage:
    def __init__(self, body):
        self.body = body


def test_parse_message_valid(worker):
    payload = {"climb_id": "C123ABC", "match_uuid": "JOB123", "test": "test2"}
    message = MockMessage(body=json.dumps(payload))
    parsed_payload, climb_id, job_uuid = worker._parse_message(message)

    assert parsed_payload == payload
    assert climb_id == "C123ABC"
    assert job_uuid == "JOB123"


def test_parse_message_fail(worker):
    message = MockMessage(body="{iaminvalidjson###''][]")
    with pytest.raises(ValueError, match="Invalid JSON"):
        worker._parse_message(message)


def test_parse_message_missing_field(worker):
    payload = {"climb_id": "C123ABC"}
    message = MockMessage(body=json.dumps(payload))
    with pytest.raises(
        ValueError, match="Message missing climb_id or match_uuid"
    ):
        worker._parse_message(message)


def test_create_result_skip(worker):
    result = worker._create_result(
        climb_id="C123ABC",
        job_uuid="JOB123",
        status="SKIPPED",
        attempt=1,
        max_attempts=3,
    )

    assert result.climb_id == "C123ABC"
    assert result.job_uuid == "JOB123"
    assert result.status == "SKIPPED"
    assert result.pipeline_name == "test-pipeline"
    assert result.duration is None


def test_create_result_with_timing(worker):
    result = worker._create_result(
        climb_id="C123ABC",
        job_uuid="JOB123",
        status="SUCCESS",
        start_time=dt.datetime.fromtimestamp(100.0, tz=dt.UTC),
        end_time=dt.datetime.fromtimestamp(105.5, tz=dt.UTC),
    )
    assert result.duration == 5.5
    assert result.start_time == "1970-01-01T00:01:40+00:00"
    assert result.end_time == "1970-01-01T00:01:45.500000+00:00"

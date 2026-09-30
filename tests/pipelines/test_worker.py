import datetime as dt
import json

import pytest
from conftest import MockMessage

from cherami.exceptions import SampleError
from cherami.pipelines.pipeline import Pipeline
from cherami.pipelines.worker import ConfigurationError, Worker


@pytest.fixture
def mock_pipeline(mocker):
    pipeline = mocker.Mock(spec=Pipeline)
    pipeline.config = mocker.Mock()
    pipeline.config.name = "test-pipeline"
    return pipeline


@pytest.fixture
def worker(mock_complete_worker_config, mock_pipeline, tmp_path):
    return Worker(
        worker_config=mock_complete_worker_config,
        pipeline=mock_pipeline,
        work_dir=tmp_path / "work",
        output_dir=tmp_path / "output",
        audit_db_path=tmp_path / "audit.db",
    )


def test_parse_message_valid(worker, message):
    parsed_payload, climb_id, job_uuid = worker._parse_message(message)
    payload = {
        "climb_id": "C-1234567890",
        "match_uuid": "JOB123",
        "test": "test",
    }
    assert parsed_payload == payload
    assert climb_id == "C-1234567890"
    assert job_uuid == "JOB123"


def test_parse_message_fail(worker, caplog):
    message = MockMessage(body="{iaminvalidjson###''][]")
    with pytest.raises(SampleError, match="invalid_message"):
        worker._parse_message(message)
    assert "Invalid JSON in varys message" in caplog.text


def test_parse_message_missing_field(worker, caplog):
    payload = {"climb_id": "C123ABC"}
    message = MockMessage(body=json.dumps(payload))
    with pytest.raises(SampleError, match="malformed_id_in_message"):
        worker._parse_message(message)
    assert "is malformed. Cannot continue." in caplog.text


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


def test_validate(worker):
    worker.validate()


def test_validate_raises_listen(worker, caplog):
    worker.listen_exchange = None
    with pytest.raises(ConfigurationError) as we:
        worker.validate()
    assert "listen_exchange_config" in str(we.value)
    assert "cannot consume messages" in caplog.text


def test_validate_raises_dead_sample(worker, caplog):
    worker.dead_sample_exchange = None
    with pytest.raises(ConfigurationError) as we:
        worker.validate()
    assert "dead_sample_exchange_config" in str(we.value)
    assert "cannot continue" in caplog.text

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cherami.pipelines.worker import PipelineResult


class AuditDB:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        ## concurrent writes are technically possible - so set some params to wait 30s to acquire a lock
        ## this should be enough time for most operations
        ## will raise an OperationalError if it can't acquire the lock in that time which is caught in the worker
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    climb_id TEXT NOT NULL,
                    job_uuid TEXT NOT NULL,
                    pipeline_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    attempt INTEGER,
                    max_attempts INTEGER,
                    start_time REAL,
                    end_time REAL,
                    duration REAL
                )
                """
            )

    def add_record(self, result: PipelineResult) -> None:
        timestamp = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO audit_log (
                    climb_id, job_uuid, pipeline_name, timestamp, status,
                    error_message, attempt, max_attempts, start_time, end_time, duration
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.climb_id,
                    result.job_uuid,
                    result.pipeline_name,
                    timestamp,
                    result.status,
                    result.error_message,
                    result.attempt,
                    result.max_attempts,
                    result.start_time,
                    result.end_time,
                    result.duration,
                ),
            )

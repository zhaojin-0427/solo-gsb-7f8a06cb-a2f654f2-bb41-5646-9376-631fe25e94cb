"""SQLite persistence for the control service.

Both control processes share this one database file (WAL mode), so
batches, confirmed receipts and leases stay consistent across them and
survive restarts. Batch rows are immutable once created (only the derived
status column is updated); receipts are appended exactly once per
(batch_id, step_no) thanks to the primary key.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time

logger = logging.getLogger("control.db")

DB_PATH = os.environ.get("CONTROL_DB", "/data/control.db")

STATUS_PENDING = "pending"            # 待确认
STATUS_COMPLETED = "completed"        # 已完成
STATUS_FAILED_FIRST = "failed_first"  # 首步失败
STATUS_PARTIAL = "partial"            # 部分完成
FINAL_STATUSES = (STATUS_COMPLETED, STATUS_FAILED_FIRST, STATUS_PARTIAL)

_lock = threading.RLock()


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with _lock:
        conn = _connect()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id    TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    channel     TEXT NOT NULL,
                    steps_json  TEXT NOT NULL,
                    total_steps INTEGER NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'completed',
                                                  'failed_first', 'partial')),
                    created_at  REAL NOT NULL,
                    updated_at  REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS receipts (
                    batch_id           TEXT NOT NULL,
                    step_no            INTEGER NOT NULL,
                    op_key             TEXT NOT NULL,
                    result             TEXT NOT NULL,
                    amount_ml          INTEGER NOT NULL,
                    remaining_after_ml INTEGER NOT NULL,
                    created_at         REAL NOT NULL,
                    PRIMARY KEY (batch_id, step_no)
                );
                CREATE TABLE IF NOT EXISTS leases (
                    batch_id   TEXT PRIMARY KEY,
                    owner      TEXT NOT NULL,
                    expires_at REAL NOT NULL
                );
                """
            )
        finally:
            conn.close()


class RequestKeyExists(Exception):
    """A batch with this request key already exists."""


def create_batch(batch_id: str, request_key: str, channel: str, steps: list[int]) -> None:
    """Persist the immutable batch row (status starts as 'pending')."""
    now = time.time()
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                """INSERT INTO batches
                   (batch_id, request_key, channel, steps_json, total_steps,
                    status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)""",
                (batch_id, request_key, channel, json.dumps(steps), len(steps), now, now),
            )
        except sqlite3.IntegrityError:
            raise RequestKeyExists(request_key)
        finally:
            conn.close()


def _batch_dict(row: sqlite3.Row) -> dict:
    return {
        "batch_id": row["batch_id"],
        "request_key": row["request_key"],
        "channel": row["channel"],
        "steps": json.loads(row["steps_json"]),
        "total_steps": row["total_steps"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def get_batch(batch_id: str) -> dict | None:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        finally:
            conn.close()
    return _batch_dict(row) if row is not None else None


def get_batch_by_request_key(request_key: str) -> dict | None:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT * FROM batches WHERE request_key = ?", (request_key,)
            ).fetchone()
        finally:
            conn.close()
    return _batch_dict(row) if row is not None else None


def list_batches(status: str | None = None) -> list[dict]:
    with _lock:
        conn = _connect()
        try:
            if status is None:
                rows = conn.execute(
                    "SELECT * FROM batches ORDER BY created_at"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM batches WHERE status = ? ORDER BY created_at",
                    (status,),
                ).fetchall()
        finally:
            conn.close()
    return [_batch_dict(row) for row in rows]


def get_receipt(batch_id: str, step_no: int) -> dict | None:
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                """SELECT step_no, op_key, result, amount_ml, remaining_after_ml,
                          created_at AS received_at
                   FROM receipts WHERE batch_id = ? AND step_no = ?""",
                (batch_id, step_no),
            ).fetchone()
        finally:
            conn.close()
    return dict(row) if row is not None else None


def get_receipts(batch_id: str) -> list[dict]:
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                """SELECT step_no, op_key, result, amount_ml, remaining_after_ml,
                          created_at AS received_at
                   FROM receipts WHERE batch_id = ? ORDER BY step_no""",
                (batch_id,),
            ).fetchall()
        finally:
            conn.close()
    return [dict(row) for row in rows]


def record_receipt(batch_id: str, step_no: int, receipt: dict, total_steps: int) -> str:
    """Persist a confirmed device receipt and recompute the batch status,
    atomically. Idempotent: the (batch_id, step_no) primary key deduplicates
    concurrent drivers, and the status is derived from the full receipt set.
    Returns the new status.
    """
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO receipts
                       (batch_id, step_no, op_key, result, amount_ml,
                        remaining_after_ml, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        batch_id,
                        step_no,
                        receipt["op_key"],
                        receipt["result"],
                        receipt["amount_ml"],
                        receipt["remaining_after_ml"],
                        time.time(),
                    ),
                )
                rows = conn.execute(
                    "SELECT step_no, result FROM receipts WHERE batch_id = ? ORDER BY step_no",
                    (batch_id,),
                ).fetchall()
                rejected = [r for r in rows if r["result"] == "rejected"]
                if rejected:
                    status = (
                        STATUS_FAILED_FIRST
                        if rejected[0]["step_no"] == 1
                        else STATUS_PARTIAL
                    )
                elif len(rows) == total_steps:
                    status = STATUS_COMPLETED
                else:
                    status = STATUS_PENDING
                conn.execute(
                    "UPDATE batches SET status = ?, updated_at = ? WHERE batch_id = ?",
                    (status, time.time(), batch_id),
                )
                conn.execute("COMMIT")
                logger.info(
                    "receipt persisted for batch %s step %s (%s) -> status=%s",
                    batch_id, step_no, receipt["result"], status,
                )
                return status
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()


def acquire_lease(batch_id: str, owner: str, ttl_seconds: float) -> bool:
    """Take or renew the drive lease for a batch. Atomic across processes."""
    now = time.time()
    expires_at = now + ttl_seconds
    with _lock:
        conn = _connect()
        try:
            cur = conn.execute(
                """INSERT INTO leases (batch_id, owner, expires_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(batch_id) DO UPDATE
                   SET owner = excluded.owner, expires_at = excluded.expires_at
                   WHERE leases.expires_at < ? OR leases.owner = excluded.owner""",
                (batch_id, owner, expires_at, now),
            )
            return cur.rowcount > 0
        finally:
            conn.close()


def release_lease(batch_id: str, owner: str) -> None:
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "DELETE FROM leases WHERE batch_id = ? AND owner = ?",
                (batch_id, owner),
            )
        finally:
            conn.close()


def unfinished_batches() -> list[dict]:
    """Non-final batches with their lease state, for the reaper."""
    with _lock:
        conn = _connect()
        try:
            rows = conn.execute(
                """SELECT b.batch_id AS batch_id,
                          l.owner AS lease_owner,
                          l.expires_at AS lease_expires_at
                   FROM batches b
                   LEFT JOIN leases l ON l.batch_id = b.batch_id
                   WHERE b.status = 'pending'"""
            ).fetchall()
        finally:
            conn.close()
    return [dict(row) for row in rows]

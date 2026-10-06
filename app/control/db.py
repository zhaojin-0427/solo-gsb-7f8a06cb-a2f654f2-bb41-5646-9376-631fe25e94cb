"""控制端 SQLite 持久化（与设备模拟器的数据库完全独立）。

单进程内复用一条连接，所有写事务使用 BEGIN IMMEDIATE 串行化；
两个控制进程之间由 SQLite WAL + busy_timeout 保证跨进程互斥。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .config import DB_PATH

_lock = threading.RLock()
_conn: Optional[sqlite3.Connection] = None


class KeyConflictError(Exception):
    """同一 request_key 已存在且批次内容不同。"""

    def __init__(self, existing_batch_id: str):
        super().__init__("request_key conflict")
        self.existing_batch_id = existing_batch_id


class LeaseLostError(Exception):
    """租约已被其它控制进程接管或过期。"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def init_db() -> None:
    global _conn
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    # 两个控制进程可能同时首次启动：WAL 模式切换需要独占访问，
    # 遇 SQLITE_BUSY 时退避重试。
    last_exc: Optional[Exception] = None
    for attempt in range(30):
        try:
            conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=15)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=15000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            # autocommit：所有事务边界由代码显式控制
            conn.isolation_level = None
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id    TEXT PRIMARY KEY,
                    request_key TEXT NOT NULL UNIQUE,
                    channel     TEXT NOT NULL,
                    steps_json  TEXT NOT NULL,
                    total_steps INTEGER NOT NULL,
                    created_at  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS receipts (
                    op_key   TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL REFERENCES batches(batch_id),
                    step_no  INTEGER NOT NULL,
                    payload  TEXT NOT NULL,
                    UNIQUE(batch_id, step_no)
                );
                CREATE INDEX IF NOT EXISTS idx_receipts_batch ON receipts(batch_id);
                CREATE TABLE IF NOT EXISTS work_state (
                    batch_id      TEXT PRIMARY KEY REFERENCES batches(batch_id),
                    step_no       INTEGER NOT NULL,
                    lease_owner   TEXT NOT NULL,
                    lease_expires REAL NOT NULL
                );
                """
            )
            _conn = conn
            return
        except sqlite3.OperationalError as exc:
            last_exc = exc
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(0.5)
    raise RuntimeError(f"控制端数据库初始化失败: {last_exc}")


def _db() -> sqlite3.Connection:
    if _conn is None:
        init_db()
    assert _conn is not None
    return _conn


# ---------- 批次 ----------

def insert_batch(request_key: str, channel: str, steps: list[int]) -> tuple[str, bool]:
    """持久化不可变批次。

    返回 (batch_id, reused)：request_key 已存在且内容相同时复用原批次；
    内容不同抛出 KeyConflictError（HTTP 409）。
    """
    with _lock:
        conn = _db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT batch_id, channel, steps_json FROM batches WHERE request_key = ?",
                (request_key,),
            ).fetchone()
            if row is not None:
                if row["channel"] == channel and json.loads(row["steps_json"]) == steps:
                    conn.execute("COMMIT")
                    return row["batch_id"], True
                conn.execute("ROLLBACK")
                raise KeyConflictError(row["batch_id"])

            batch_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO batches VALUES (?, ?, ?, ?, ?, ?)",
                (batch_id, request_key, channel, json.dumps(steps), len(steps), _now_iso()),
            )
            conn.execute("COMMIT")
            return batch_id, False
        except KeyConflictError:
            raise
        except Exception:
            conn.execute("ROLLBACK")
            raise


def get_batch(batch_id: str) -> Optional[dict[str, Any]]:
    with _lock:
        row = _db().execute(
            "SELECT * FROM batches WHERE batch_id = ?", (batch_id,)
        ).fetchone()
    return _batch_row_to_dict(row) if row else None


def list_all_batches() -> list[dict[str, Any]]:
    with _lock:
        rows = _db().execute(
            "SELECT * FROM batches ORDER BY created_at, batch_id"
        ).fetchall()
    return [_batch_row_to_dict(r) for r in rows]


def _batch_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "batch_id": row["batch_id"],
        "request_key": row["request_key"],
        "channel": row["channel"],
        "steps": json.loads(row["steps_json"]),
        "total_steps": row["total_steps"],
        "created_at": row["created_at"],
    }


# ---------- 回执 ----------

def get_receipts(batch_id: str) -> list[dict[str, Any]]:
    with _lock:
        rows = _db().execute(
            "SELECT op_key, step_no, payload FROM receipts WHERE batch_id = ? ORDER BY step_no",
            (batch_id,),
        ).fetchall()
    return [
        {"op_key": r["op_key"], "step_no": r["step_no"], "receipt": json.loads(r["payload"])}
        for r in rows
    ]


# ---------- worker 租约 ----------

def _is_terminal(receipt_rows: list[sqlite3.Row], total_steps: int) -> bool:
    if len(receipt_rows) >= total_steps:
        return True
    return any(json.loads(r["payload"]).get("status") == "rejected" for r in receipt_rows)


def claim_next_step(batch_id: str, worker_id: str, ttl: float) -> Optional[dict[str, Any]]:
    """领取该批次的下一个顺序步骤并持有租约。

    - 批次已完成 / 已被拒绝（首步失败或余量不足终止）-> None
    - 租约被其它未失联 worker 持有 -> None
    - 无租约 / 自己持有 / 他人租约过期 -> 领取（或接管）下一步
    """
    with _lock:
        conn = _db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            batch = conn.execute(
                "SELECT total_steps FROM batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
            if batch is None:
                conn.execute("ROLLBACK")
                return None
            receipts = conn.execute(
                "SELECT payload FROM receipts WHERE batch_id = ? ORDER BY step_no",
                (batch_id,),
            ).fetchall()
            if _is_terminal(receipts, batch["total_steps"]):
                conn.execute("COMMIT")
                return None

            next_step = len(receipts) + 1
            ws = conn.execute(
                "SELECT step_no, lease_owner, lease_expires FROM work_state WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            now = time.time()
            if ws is not None and ws["lease_owner"] != worker_id and ws["lease_expires"] > now:
                # 别的活进程正在驱动该批次
                conn.execute("COMMIT")
                return None

            conn.execute(
                """
                INSERT INTO work_state(batch_id, step_no, lease_owner, lease_expires)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(batch_id) DO UPDATE SET
                    step_no = excluded.step_no,
                    lease_owner = excluded.lease_owner,
                    lease_expires = excluded.lease_expires
                """,
                (batch_id, next_step, worker_id, now + ttl),
            )
            conn.execute("COMMIT")
            return {"step_no": next_step}
        except Exception:
            conn.execute("ROLLBACK")
            raise


def complete_step(
    batch_id: str, step_no: int, op_key: str, receipt: dict[str, Any], worker_id: str
) -> None:
    """回执落库并释放租约。仅租约持有者可提交，防止接管方与原进程重复推进。

    回执落库与释放租约在同一事务内，保证“回执未落库绝不发送下一步”。
    """
    with _lock:
        conn = _db()
        conn.execute("BEGIN IMMEDIATE")
        try:
            ws = conn.execute(
                "SELECT lease_owner, step_no FROM work_state WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if ws is None:
                # 租约已释放：可能该回执已被自己提前落库，幂等确认即可
                existing = conn.execute(
                    "SELECT 1 FROM receipts WHERE op_key = ?", (op_key,)
                ).fetchone()
                conn.execute("COMMIT")
                if existing:
                    return
                raise LeaseLostError("no active lease")
            if ws["lease_owner"] != worker_id or ws["step_no"] != step_no:
                conn.execute("COMMIT")
                raise LeaseLostError("lease taken over")

            conn.execute(
                "INSERT OR IGNORE INTO receipts(op_key, batch_id, step_no, payload) "
                "VALUES(?, ?, ?, ?)",
                (op_key, batch_id, step_no, json.dumps(receipt, ensure_ascii=False)),
            )
            conn.execute("DELETE FROM work_state WHERE batch_id = ?", (batch_id,))
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

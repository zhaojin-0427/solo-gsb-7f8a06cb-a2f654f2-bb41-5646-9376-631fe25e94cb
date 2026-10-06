"""设备模拟器 SQLite 持久化（与控制端数据库完全独立）。

每次操作使用独立连接，余额扣减 / 拒绝回执 / 暂停标记在单条
BEGIN IMMEDIATE 事务内原子完成；跨通道互不阻塞写事务的业务语义。
"""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from .config import INITIAL_BALANCE_ML, DB_PATH

VALID_CHANNELS = ("A", "B")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS balances (
                channel      TEXT PRIMARY KEY,
                remaining_ml INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS operations (
                op_key              TEXT PRIMARY KEY,
                batch_id            TEXT NOT NULL,
                channel             TEXT NOT NULL,
                step_no             INTEGER NOT NULL,
                amount_ml           INTEGER NOT NULL,
                status              TEXT NOT NULL,
                remaining_ml_after  INTEGER NOT NULL,
                receipt_id          TEXT NOT NULL,
                held                INTEGER NOT NULL DEFAULT 0,
                created_at          TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pause_arms (
                id         INTEGER PRIMARY KEY CHECK (id = 1),
                channel    TEXT NOT NULL,
                armed_at   TEXT NOT NULL
            );
            """
        )
        for channel in VALID_CHANNELS:
            conn.execute(
                "INSERT OR IGNORE INTO balances(channel, remaining_ml) VALUES(?, ?)",
                (channel, INITIAL_BALANCE_ML),
            )
    finally:
        conn.close()


class ParamConflictError(Exception):
    """同操作键已存在但参数不同（HTTP 409）。"""

    def __init__(self, existing: dict[str, Any]):
        super().__init__("operation key parameter conflict")
        self.existing = existing


def apply_dispense(
    op_key: str, batch_id: str, channel: str, step_no: int, amount_ml: int
) -> tuple[dict[str, Any], Optional[str]]:
    """原子处理一次加液：幂等命中 / 参数冲突 / 成功扣量（可挂起回执）/ 余量不足拒绝。

    返回 (receipt, hold_state):
      * 幂等命中历史操作 -> (已有回执, "existing_held" 或 None)
      * 成功或拒绝 -> (新回执, "new_hold" 或 None)
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            "SELECT * FROM operations WHERE op_key = ?", (op_key,)
        ).fetchone()
        if existing is not None:
            params = (existing["batch_id"], existing["channel"], existing["step_no"], existing["amount_ml"])
            if params != (batch_id, channel, step_no, amount_ml):
                conn.execute("ROLLBACK")
                raise ParamConflictError(_row_to_receipt(existing))
            conn.execute("COMMIT")
            hold_state = "existing_held" if existing["held"] else None
            return _row_to_receipt(existing), hold_state

        row = conn.execute(
            "SELECT remaining_ml FROM balances WHERE channel = ?", (channel,)
        ).fetchone()
        remaining = row["remaining_ml"]
        now = _now_iso()
        arm = conn.execute("SELECT channel FROM pause_arms WHERE id = 1").fetchone()
        hold_on_success = arm is not None and arm["channel"] == channel

        if remaining >= amount_ml:
            new_remaining = remaining - amount_ml
            status = "success"
            held = 1 if hold_on_success else 0
            conn.execute(
                "UPDATE balances SET remaining_ml = ? WHERE channel = ?",
                (new_remaining, channel),
            )
        else:
            # 余量不足：拒绝且不扣量，设备实际余量不变；
            # 暂停臂保留（尚未出现“成功扣量”，等下一次成功再触发）
            new_remaining = remaining
            status = "rejected"
            held = 0

        receipt_id = uuid.uuid4().hex
        conn.execute(
            "INSERT INTO operations(op_key, batch_id, channel, step_no, amount_ml, "
            "status, remaining_ml_after, receipt_id, held, created_at) "
            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (op_key, batch_id, channel, step_no, amount_ml, status,
             new_remaining, receipt_id, held, now),
        )
        if status == "success" and held:
            # 一次性暂停臂：下一次成功扣量已消费
            conn.execute("DELETE FROM pause_arms WHERE id = 1")
        conn.execute("COMMIT")
        receipt = {
            "receipt_id": receipt_id,
            "op_key": op_key,
            "batch_id": batch_id,
            "channel": channel,
            "step_no": step_no,
            "amount_ml": amount_ml,
            "status": status,
            "remaining_ml_after": new_remaining,
            "created_at": now,
        }
        if status == "rejected":
            receipt["reason"] = "insufficient_balance"
        return receipt, ("new_hold" if held else None)
    except ParamConflictError:
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def get_operation(op_key: str) -> Optional[dict[str, Any]]:
    conn = _connect()
    try:
        row = conn.execute("SELECT * FROM operations WHERE op_key = ?", (op_key,)).fetchone()
        return _row_to_receipt(row) if row else None
    finally:
        conn.close()


def _row_to_receipt(row: sqlite3.Row) -> dict[str, Any]:
    receipt = {
        "receipt_id": row["receipt_id"],
        "op_key": row["op_key"],
        "batch_id": row["batch_id"],
        "channel": row["channel"],
        "step_no": row["step_no"],
        "amount_ml": row["amount_ml"],
        "status": row["status"],
        "remaining_ml_after": row["remaining_ml_after"],
        "created_at": row["created_at"],
        "held": bool(row["held"]),
    }
    if row["status"] == "rejected":
        receipt["reason"] = "insufficient_balance"
    return receipt


def release_op(op_key: str) -> bool:
    """手动释放被挂起的操作键回执，返回是否曾存在挂起记录。"""
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "UPDATE operations SET held = 0 WHERE op_key = ? AND held = 1", (op_key,)
        )
        released = cur.rowcount == 1
        conn.execute("COMMIT")
        return released
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def arm_pause(channel: str) -> None:
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO pause_arms(id, channel, armed_at) VALUES(1, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET channel = excluded.channel, armed_at = excluded.armed_at",
            (channel, _now_iso()),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def disarm_pause() -> None:
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM pause_arms WHERE id = 1")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def get_pause_arm() -> Optional[dict[str, str]]:
    conn = _connect()
    try:
        row = conn.execute("SELECT channel, armed_at FROM pause_arms WHERE id = 1").fetchone()
        return {"channel": row["channel"], "armed_at": row["armed_at"]} if row else None
    finally:
        conn.close()


def get_balances() -> dict[str, int]:
    conn = _connect()
    try:
        rows = conn.execute("SELECT channel, remaining_ml FROM balances").fetchall()
        return {r["channel"]: r["remaining_ml"] for r in rows}
    finally:
        conn.close()


def list_operations(channel: Optional[str] = None) -> list[dict[str, Any]]:
    conn = _connect()
    try:
        if channel:
            rows = conn.execute(
                "SELECT * FROM operations WHERE channel = ? ORDER BY created_at, op_key",
                (channel,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM operations ORDER BY created_at, op_key"
            ).fetchall()
        return [_row_to_receipt(r) for r in rows]
    finally:
        conn.close()

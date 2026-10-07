"""SQLite persistence for the device simulator.

One independent database file holds the channel balances, the idempotent
operation log (receipts) and the pause-arm state. Every multi-statement
write runs inside a BEGIN IMMEDIATE transaction guarded by a process-wide
lock, so a deduction (or an insufficient-balance rejection) and its
receipt are always persisted atomically and survive restarts.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
import time

logger = logging.getLogger("device.db")

DB_PATH = os.environ.get("DEVICE_DB", "/data/device.db")
INITIAL_VOLUME_ML = 100
CHANNELS = ("A", "B")

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
                CREATE TABLE IF NOT EXISTS channels (
                    name         TEXT PRIMARY KEY,
                    remaining_ml INTEGER NOT NULL CHECK (remaining_ml >= 0)
                );
                CREATE TABLE IF NOT EXISTS operations (
                    op_key             TEXT PRIMARY KEY,
                    batch_id           TEXT NOT NULL,
                    step_no            INTEGER NOT NULL,
                    channel            TEXT NOT NULL,
                    amount_ml          INTEGER NOT NULL,
                    result             TEXT NOT NULL
                                       CHECK (result IN ('success', 'rejected')),
                    remaining_after_ml INTEGER NOT NULL,
                    held               INTEGER NOT NULL DEFAULT 0,
                    created_at         REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS pause_arms (
                    channel  TEXT PRIMARY KEY,
                    armed_at REAL NOT NULL
                );
                """
            )
            for channel in CHANNELS:
                conn.execute(
                    "INSERT OR IGNORE INTO channels (name, remaining_ml) VALUES (?, ?)",
                    (channel, INITIAL_VOLUME_ML),
                )
        finally:
            conn.close()


def _receipt(row: sqlite3.Row) -> dict:
    return {
        "op_key": row["op_key"],
        "batch_id": row["batch_id"],
        "step_no": row["step_no"],
        "channel": row["channel"],
        "amount_ml": row["amount_ml"],
        "result": row["result"],
        "remaining_after_ml": row["remaining_after_ml"],
        "created_at": row["created_at"],
    }


class ConflictError(Exception):
    """The operation key already exists with different parameters."""


def apply_dispense(
    op_key: str,
    batch_id: str,
    step_no: int,
    channel: str,
    amount_ml: int,
) -> tuple[dict, bool]:
    """Atomically persist the outcome of one dosing operation.

    Returns (receipt, held). Replaying an existing key with identical
    parameters returns the stored receipt without touching the balance;
    replaying with different parameters raises ConflictError.
    """
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                row = conn.execute(
                    "SELECT * FROM operations WHERE op_key = ?", (op_key,)
                ).fetchone()
                if row is not None:
                    same_params = (
                        row["batch_id"] == batch_id
                        and row["step_no"] == step_no
                        and row["channel"] == channel
                        and row["amount_ml"] == amount_ml
                    )
                    if not same_params:
                        raise ConflictError(
                            f"operation key {op_key!r} already used with different parameters"
                        )
                    conn.execute("COMMIT")
                    return _receipt(row), bool(row["held"])

                remaining = conn.execute(
                    "SELECT remaining_ml FROM channels WHERE name = ?", (channel,)
                ).fetchone()["remaining_ml"]

                if remaining >= amount_ml:
                    result = "success"
                    remaining_after = remaining - amount_ml
                    conn.execute(
                        "UPDATE channels SET remaining_ml = ? WHERE name = ?",
                        (remaining_after, channel),
                    )
                else:
                    result = "rejected"
                    remaining_after = remaining

                held = 0
                if result == "success":
                    armed = conn.execute(
                        "SELECT channel FROM pause_arms WHERE channel = ?", (channel,)
                    ).fetchone()
                    if armed is not None:
                        # Pause the receipt of this operation key: the deduction
                        # is persisted, the receipt is held until manual release.
                        held = 1
                        conn.execute(
                            "DELETE FROM pause_arms WHERE channel = ?", (channel,)
                        )

                conn.execute(
                    """INSERT INTO operations
                       (op_key, batch_id, step_no, channel, amount_ml, result,
                        remaining_after_ml, held, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        op_key,
                        batch_id,
                        step_no,
                        channel,
                        amount_ml,
                        result,
                        remaining_after,
                        held,
                        time.time(),
                    ),
                )
                conn.execute("COMMIT")
                logger.info(
                    "op=%s channel=%s amount=%s result=%s remaining=%s held=%s",
                    op_key, channel, amount_ml, result, remaining_after, held,
                )
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    row = get_operation(op_key)
    return row[0], row[1]


def get_operation(op_key: str) -> tuple[dict, bool] | None:
    """Return (receipt, held) for an operation key, or None if unknown."""
    with _lock:
        conn = _connect()
        try:
            row = conn.execute(
                "SELECT * FROM operations WHERE op_key = ?", (op_key,)
            ).fetchone()
        finally:
            conn.close()
    if row is None:
        return None
    return _receipt(row), bool(row["held"])


def arm_pause(channel: str) -> None:
    """Arm a pause: the receipt of the next successful deduction on this
    channel will be held until a manual release."""
    with _lock:
        conn = _connect()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO pause_arms (channel, armed_at) VALUES (?, ?)",
                (channel, time.time()),
            )
        finally:
            conn.close()
    logger.info("pause armed on channel %s", channel)


def release_held(channel: str | None = None) -> list[str]:
    """Manually release held receipts (one channel, or all when None)."""
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                if channel is None:
                    rows = conn.execute(
                        "SELECT op_key FROM operations WHERE held = 1"
                    ).fetchall()
                    conn.execute("UPDATE operations SET held = 0 WHERE held = 1")
                else:
                    rows = conn.execute(
                        "SELECT op_key FROM operations WHERE held = 1 AND channel = ?",
                        (channel,),
                    ).fetchall()
                    conn.execute(
                        "UPDATE operations SET held = 0 WHERE held = 1 AND channel = ?",
                        (channel,),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    released = [row["op_key"] for row in rows]
    logger.info("released held receipts: %s", released)
    return released


class HeldOperationsExist(Exception):
    """Reset was requested while receipts are still held."""


def reset() -> None:
    """Test-bench facility: restore both channels to the initial volume and
    clear the operation log and pause state. Refused while receipts are
    held, so only call it when the rig is idle."""
    with _lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                held = conn.execute(
                    "SELECT COUNT(*) AS n FROM operations WHERE held = 1"
                ).fetchone()["n"]
                if held:
                    raise HeldOperationsExist(f"{held} held receipt(s); release them first")
                conn.execute("DELETE FROM operations")
                conn.execute("DELETE FROM pause_arms")
                for channel in CHANNELS:
                    conn.execute(
                        "UPDATE channels SET remaining_ml = ? WHERE name = ?",
                        (INITIAL_VOLUME_ML, channel),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    logger.warning("device state reset to %s mL per channel", INITIAL_VOLUME_ML)


def channel_states() -> dict:
    with _lock:
        conn = _connect()
        try:
            balances = {
                row["name"]: row["remaining_ml"]
                for row in conn.execute("SELECT name, remaining_ml FROM channels")
            }
            armed = {
                row["channel"]
                for row in conn.execute("SELECT channel FROM pause_arms")
            }
            held_rows = conn.execute(
                "SELECT op_key, channel FROM operations WHERE held = 1"
            ).fetchall()
        finally:
            conn.close()
    states = {}
    for channel in CHANNELS:
        states[channel] = {
            "remaining_ml": balances[channel],
            "pause_armed": channel in armed,
            "held_op_keys": [r["op_key"] for r in held_rows if r["channel"] == channel],
        }
    return states

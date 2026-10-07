"""Asynchronous batch driver.

Each batch is driven strictly in order: a step is only sent to the device
after the previous receipt has been persisted in the control database.
Device operations are keyed by "<batch_id>:<step_no>" and are idempotent,
so a driver that loses contact, crashes or loses its lease can safely
re-ask for the same operation key — the device replays the stored receipt
instead of deducting twice.

A per-batch lease (shared SQLite table) coordinates the two control
processes: while one process drives a batch it renews the lease; if it
dies, the lease expires and the other process's reaper takes over exactly
where the batch left off.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import httpx

from . import db

logger = logging.getLogger("control.driver")

DEVICE_URL = os.environ.get("DEVICE_URL", "http://device:8000").rstrip("/")
OWNER_ID = os.environ.get("CONTROL_ID") or f"control-{os.getpid()}"

LEASE_TTL_SECONDS = float(os.environ.get("LEASE_TTL_SECONDS", "20"))
LEASE_RENEW_INTERVAL_SECONDS = float(os.environ.get("LEASE_RENEW_INTERVAL_SECONDS", "5"))
REAPER_INTERVAL_SECONDS = float(os.environ.get("REAPER_INTERVAL_SECONDS", "2"))
DEVICE_TIMEOUT_SECONDS = float(os.environ.get("DEVICE_TIMEOUT_SECONDS", "30"))
DEVICE_RETRY_BACKOFF_SECONDS = float(os.environ.get("DEVICE_RETRY_BACKOFF_SECONDS", "1"))

_client: httpx.AsyncClient | None = None
_local_tasks: dict[str, asyncio.Task] = {}


def init() -> None:
    global _client
    _client = httpx.AsyncClient(timeout=DEVICE_TIMEOUT_SECONDS)


async def aclose() -> None:
    for task in list(_local_tasks.values()):
        task.cancel()
    if _local_tasks:
        await asyncio.gather(*_local_tasks.values(), return_exceptions=True)
    if _client is not None:
        await _client.aclose()


def op_key_of(batch_id: str, step_no: int) -> str:
    return f"{batch_id}:{step_no}"


async def _dispense_with_retry(
    batch_id: str, step_no: int, channel: str, amount_ml: int
) -> dict:
    """Call the device until it answers. Safe to retry: the device dedupes
    by operation key, so a retry after a lost response never deducts twice.
    A held receipt (paused channel) simply makes the device answer late."""
    payload = {
        "op_key": op_key_of(batch_id, step_no),
        "batch_id": batch_id,
        "step_no": step_no,
        "channel": channel,
        "amount_ml": amount_ml,
    }
    while True:
        try:
            resp = await _client.post(f"{DEVICE_URL}/dispense", json=payload)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 409:
                # Must not happen: parameters of an operation key are immutable.
                logger.error(
                    "device reports 409 conflict for %s: %s",
                    payload["op_key"], resp.text,
                )
            else:
                logger.warning(
                    "device answered %s for %s: %s",
                    resp.status_code, payload["op_key"], resp.text,
                )
        except httpx.HTTPError as exc:
            logger.warning(
                "device call for %s failed (%s); retrying with the same operation key",
                payload["op_key"], exc,
            )
        await asyncio.sleep(DEVICE_RETRY_BACKOFF_SECONDS)


async def _renew_lease_loop(batch_id: str, stop: asyncio.Event) -> None:
    while not stop.is_set():
        try:
            db.acquire_lease(batch_id, OWNER_ID, LEASE_TTL_SECONDS)
        except Exception:
            logger.exception("lease renewal failed for batch %s", batch_id)
        try:
            await asyncio.wait_for(stop.wait(), timeout=LEASE_RENEW_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass


async def drive_batch(batch_id: str) -> None:
    """Drive one batch to a final state, confirming steps strictly in order."""
    try:
        batch = db.get_batch(batch_id)
        if batch is None or batch["status"] in db.FINAL_STATUSES:
            return
        steps = batch["steps"]
        channel = batch["channel"]
        logger.info("driving batch %s (%s, steps=%s)", batch_id, channel, steps)

        stop = asyncio.Event()
        renewer = asyncio.create_task(_renew_lease_loop(batch_id, stop))
        try:
            for step_no in range(1, len(steps) + 1):
                if not db.acquire_lease(batch_id, OWNER_ID, LEASE_TTL_SECONDS):
                    logger.info(
                        "lease for batch %s held elsewhere; stopping local driver",
                        batch_id,
                    )
                    return
                receipt = db.get_receipt(batch_id, step_no)
                if receipt is None:
                    receipt = await _dispense_with_retry(
                        batch_id, step_no, channel, steps[step_no - 1]
                    )
                    # The next step is only sent after this receipt is persisted.
                    db.record_receipt(batch_id, step_no, receipt, len(steps))
                if receipt["result"] == "rejected":
                    # Insufficient balance: stop, keep what was already deducted.
                    return
        finally:
            stop.set()
            await renewer
            db.release_lease(batch_id, OWNER_ID)
        logger.info("batch %s finished with status %s", batch_id, db.get_batch(batch_id)["status"])
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("driver for batch %s crashed; the reaper will resume it", batch_id)
    finally:
        _local_tasks.pop(batch_id, None)


def kick_batch(batch_id: str) -> None:
    """Start a local driver for a batch unless one is already running."""
    task = _local_tasks.get(batch_id)
    if task is not None and not task.done():
        return
    if db.acquire_lease(batch_id, OWNER_ID, LEASE_TTL_SECONDS):
        _local_tasks[batch_id] = asyncio.create_task(drive_batch(batch_id))


async def reaper_loop() -> None:
    """Resume batches whose driver vanished (crash, restart, expired lease)."""
    while True:
        await asyncio.sleep(REAPER_INTERVAL_SECONDS)
        try:
            now = time.time()
            for row in db.unfinished_batches():
                batch_id = row["batch_id"]
                task = _local_tasks.get(batch_id)
                if task is not None and not task.done():
                    continue
                lease_busy = (
                    row["lease_expires_at"] is not None
                    and row["lease_expires_at"] > now
                    and row["lease_owner"] != OWNER_ID
                )
                if lease_busy:
                    continue
                if db.acquire_lease(batch_id, OWNER_ID, LEASE_TTL_SECONDS):
                    logger.info("reaper: (re)claiming batch %s", batch_id)
                    _local_tasks[batch_id] = asyncio.create_task(drive_batch(batch_id))
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("reaper iteration failed")


async def get_channel_remaining(channel: str) -> int | None:
    """Actual device balance for a channel; None if the device is unreachable."""
    if _client is None:
        return None
    try:
        resp = await _client.get(f"{DEVICE_URL}/channels", timeout=3.0)
        resp.raise_for_status()
        return resp.json()[channel]["remaining_ml"]
    except Exception:
        logger.warning("could not fetch device balance for channel %s", channel)
        return None

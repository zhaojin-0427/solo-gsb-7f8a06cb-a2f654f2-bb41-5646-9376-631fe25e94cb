"""Control service HTTP API.

Accepts dosing batches, persists them immutably, returns the batch id
immediately and drives the device asynchronously. Two control processes
share one SQLite database and coordinate through leases, so requests can
be served by either process and batches survive crashes and restarts.
"""

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Response

from . import db, driver
from .schemas import BatchCreate, BatchStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("control")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    driver.init()
    reaper = asyncio.create_task(driver.reaper_loop())
    logger.info("control service ready, db=%s, owner=%s", db.DB_PATH, driver.OWNER_ID)
    yield
    reaper.cancel()
    await driver.aclose()


app = FastAPI(title="Dosing control service", version="1.0.0", lifespan=lifespan)


def _summary(batch: dict) -> dict:
    return {
        "batch_id": batch["batch_id"],
        "request_key": batch["request_key"],
        "channel": batch["channel"],
        "steps": batch["steps"],
        "status": batch["status"],
        "created_at": batch["created_at"],
        "updated_at": batch["updated_at"],
    }


def _replay_or_conflict(existing: dict, req: BatchCreate, response: Response) -> dict:
    same_content = (
        existing["channel"] == req.channel and existing["steps"] == list(req.steps)
    )
    if not same_content:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "request_key already used with different content",
                "batch_id": existing["batch_id"],
            },
        )
    response.status_code = 200
    return _summary(existing)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/batches", status_code=202)
async def create_batch(req: BatchCreate, response: Response) -> dict:
    """Persist the immutable batch, return its id, drive it asynchronously.

    Idempotent on request_key: same content -> the original batch (200),
    different content -> 409.
    """
    existing = db.get_batch_by_request_key(req.request_key)
    if existing is not None:
        return _replay_or_conflict(existing, req, response)

    batch_id = uuid.uuid4().hex
    try:
        db.create_batch(batch_id, req.request_key, req.channel, list(req.steps))
    except db.RequestKeyExists:
        # Lost a create race against the other control process.
        existing = db.get_batch_by_request_key(req.request_key)
        return _replay_or_conflict(existing, req, response)

    driver.kick_batch(batch_id)
    return _summary(db.get_batch(batch_id))


@app.get("/batches")
def list_batches(status: BatchStatus | None = None) -> list[dict]:
    return [_summary(batch) for batch in db.list_batches(status)]


@app.get("/batches/{batch_id}")
async def get_batch(batch_id: str) -> dict:
    """Batch detail: status, confirmed receipts and the actual device balance."""
    batch = db.get_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="unknown batch_id")
    return {
        **_summary(batch),
        "confirmed_receipts": db.get_receipts(batch_id),
        "device_remaining_ml": await driver.get_channel_remaining(batch["channel"]),
    }

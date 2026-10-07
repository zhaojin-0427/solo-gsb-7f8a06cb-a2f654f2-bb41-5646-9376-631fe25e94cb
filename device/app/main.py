"""Device simulator HTTP API.

Two independent channels (A and B), 100 mL each. Each dosing operation is
keyed by (batch_id, step_no); the outcome and its receipt are persisted
atomically. A pause can be armed per channel: the receipt of the next
successful deduction is then held (the deduction itself is persisted)
until a manual release, and retries of the same operation key keep
waiting as well. Other channels are unaffected.
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from . import db
from .schemas import DispenseRequest, PauseRequest, ReleaseRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("device")

HOLD_POLL_INTERVAL_SECONDS = 0.2


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    logger.info("device simulator ready, db=%s", db.DB_PATH)
    yield


app = FastAPI(title="Dosing device simulator", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/dispense")
async def dispense(req: DispenseRequest) -> dict:
    """Apply one dosing step.

    Same operation key + same parameters -> stored receipt replayed (no
    double deduction). Same key + different parameters -> 409. While the
    receipt is held (paused channel), the response is only sent after a
    manual release, however often the caller retries.
    """
    try:
        receipt, held = db.apply_dispense(
            op_key=req.op_key,
            batch_id=req.batch_id,
            step_no=req.step_no,
            channel=req.channel,
            amount_ml=req.amount_ml,
        )
    except db.ConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    if held:
        logger.info("receipt for op %s is held; waiting for manual release", req.op_key)
    while held:
        await asyncio.sleep(HOLD_POLL_INTERVAL_SECONDS)
        operation = db.get_operation(req.op_key)
        if operation is None:  # pragma: no cover - cannot happen
            raise HTTPException(status_code=500, detail="operation disappeared")
        receipt, held = operation
    return receipt


@app.post("/pause")
def pause(req: PauseRequest) -> dict:
    """Arm a pause: hold the receipt of the next successful deduction on
    this channel until a manual release."""
    db.arm_pause(req.channel)
    return {"channel": req.channel, "pause_armed": True}


@app.post("/release")
def release(req: ReleaseRequest) -> dict:
    """Manually release held receipts (one channel, or all when omitted)."""
    released = db.release_held(req.channel)
    return {"released_op_keys": released}


@app.post("/reset")
def reset() -> dict:
    """Test-bench facility: restore both channels to 100 mL and clear the
    operation log and pause state. Only for an idle rig: returns 409 while
    receipts are held."""
    try:
        db.reset()
    except db.HeldOperationsExist as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return {"reset": True, "channels": db.channel_states()}


@app.get("/channels")
def channels() -> dict:
    """Actual balances, pause-arm flags and held operation keys."""
    return db.channel_states()


@app.get("/operations/{op_key}")
def operation(op_key: str) -> dict:
    op = db.get_operation(op_key)
    if op is None:
        raise HTTPException(status_code=404, detail="unknown operation key")
    receipt, held = op
    return {**receipt, "held": held}

"""控制服务 HTTP API。

POST /batches            提交加液批次（不可变持久化后立即返回 202，异步驱动）
GET  /batches            查询批次列表（可按状态过滤）
GET  /batches/{id}       查询单个批次（状态、已确认回执、设备实际余量）
GET  /balances           设备两通道实际余量
GET  /healthz            健康检查
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from . import db, device_client
from .config import WORKER_ID
from .models import CreateBatchRequest
from .status import STATUS_TEXT, summarize
from .worker import run_forever

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("control")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    task = asyncio.create_task(run_forever(), name=f"worker-{WORKER_ID}")
    logger.info("控制服务 %s 已启动", WORKER_ID)
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="加液批次控制服务", version="1.0.0", lifespan=lifespan)


def _conflict(detail: dict[str, Any]) -> JSONResponse:
    return JSONResponse(status_code=409, content={"error": "conflict", **detail})


@app.post("/batches", status_code=202)
async def create_batch(req: CreateBatchRequest):
    """持久化不可变批次并立即返回批次 ID；设备驱动由后台 worker 异步执行。

    同一 request_key + 同内容 -> 返回原批次（reused=true）；
    同一 request_key + 不同内容 -> 409。
    """
    try:
        batch_id, reused = await asyncio.to_thread(
            db.insert_batch, req.request_key, req.channel, req.steps
        )
    except db.KeyConflictError as exc:
        return _conflict(
            {
                "message": "同一请求键已绑定不同内容的批次",
                "request_key": req.request_key,
                "existing_batch_id": exc.existing_batch_id,
            }
        )
    return {
        "batch_id": batch_id,
        "reused": reused,
        "status": "pending",
        "status_text": STATUS_TEXT["pending"],
        "message": "批次已持久化，设备异步执行中",
    }


@app.get("/batches")
async def list_batches(status: Optional[str] = Query(default=None)):
    balances = await device_client.get_balances()
    batches = await asyncio.to_thread(db.list_all_batches)

    async def _view(batch: dict[str, Any]) -> dict[str, Any]:
        receipts = await asyncio.to_thread(db.get_receipts, batch["batch_id"])
        return summarize(batch, receipts, balances)

    views = await asyncio.gather(*[_view(b) for b in batches])
    counts: dict[str, int] = {}
    for view in views:
        counts[view["status"]] = counts.get(view["status"], 0) + 1
    items = [v for v in views if status is None or v["status"] == status]
    return {
        "worker_id": WORKER_ID,
        "status_filter": status,
        "counts": counts,
        "batches": items,
        "device_balances": balances,
    }


@app.get("/batches/{batch_id}")
async def get_batch(batch_id: str):
    batch = await asyncio.to_thread(db.get_batch, batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    balances = await device_client.get_balances()
    receipts = await asyncio.to_thread(db.get_receipts, batch_id)
    return summarize(batch, receipts, balances)


@app.get("/balances")
async def balances():
    return await device_client.get_balances()


@app.get("/healthz")
async def healthz():
    return {"ok": True, "worker_id": WORKER_ID}

"""本地设备模拟器 HTTP API。

POST /dispense                      原子扣量或拒绝；成功扣量后可按暂停配置挂起回执
GET  /balances                      A/B 通道实际余量
GET  /operations                    设备侧持久化的全部操作/回执（可按通道过滤）
GET  /operations/{op_key}           查询单个操作键
POST /admin/pause/{channel}         一次性暂停臂：该通道下一次成功扣量后挂起回执
POST /admin/resume/{op_key}         手动释放被挂起的操作键，返回回执
DELETE /admin/pause                 撤销尚未触发的暂停臂
GET  /admin/state                   暂停臂与内存中挂起键状态
GET  /healthz                       健康检查
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import Any, Optional

import anyio
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from . import db as devdb
from . import hold
from .models import DispenseRequest

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("device")


@asynccontextmanager
async def lifespan(app: FastAPI):
    devdb.init_db()
    logger.info("设备模拟器已启动，初始余量 %s", devdb.get_balances())
    yield


app = FastAPI(title="本地加液设备模拟器", version="1.0.0", lifespan=lifespan)


def _conflict(existing: dict[str, Any]) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"error": "operation_key_conflict", "message": "同一操作键已绑定不同参数",
                 "existing_receipt": {k: v for k, v in existing.items() if k != "held"}},
    )


@app.post("/dispense")
async def dispense(req: DispenseRequest, request: Request):
    """以 (op_key, 参数) 为操作键的幂等加液接口。

    * 同键同参数 -> 只重放同一张回执（挂起中则继续等待手动释放）；
    * 同键不同参数 -> 409；
    * 余量不足 -> 200 返回 status=rejected 的拒绝回执（不扣量）；
    * 暂停臂命中且成功扣量 -> 扣量原子落库，但回执挂起直到 /admin/resume/{op_key}。
    """
    try:
        receipt, hold_state = await anyio.to_thread.run_sync(
            devdb.apply_dispense,
            req.op_key, req.batch_id, req.channel, req.step_no, req.amount_ml,
        )
    except devdb.ParamConflictError as exc:
        return _conflict(exc.existing)

    if hold_state is not None:
        outcome = await hold.gate(req.op_key, hold_state, receipt["created_at"], request)
        if outcome == "gone":
            # 连接已断（控制端读超时）：不返回；调用方稍后凭同一 op_key 重放即可
            return JSONResponse(status_code=503, content={"error": "client_gone", "op_key": req.op_key})
        if outcome in ("released", "stale"):
            # 释放后重查，返回持久化的同一张回执
            latest = devdb.get_operation(req.op_key)
            return latest or receipt

    return receipt


@app.get("/balances")
async def balances():
    return devdb.get_balances()


@app.get("/operations")
async def operations(channel: Optional[str] = Query(default=None)):
    if channel is not None and channel not in devdb.VALID_CHANNELS:
        raise HTTPException(status_code=422, detail="channel 必须为 A 或 B")
    items = devdb.list_operations(channel)
    return {"count": len(items), "operations": items}


@app.get("/operations/{op_key}")
async def operation(op_key: str):
    op = devdb.get_operation(op_key)
    if op is None:
        raise HTTPException(status_code=404, detail="操作键不存在")
    return op


@app.post("/admin/pause/{channel}")
async def pause(channel: str):
    """一次性暂停臂：该通道下一次“成功扣量”后，回执挂起等待手动释放。"""
    if channel not in devdb.VALID_CHANNELS:
        raise HTTPException(status_code=422, detail="channel 必须为 A 或 B")
    devdb.arm_pause(channel)
    return {"armed": True, "channel": channel,
            "message": f"通道 {channel} 下一次成功扣量后回执将被挂起"}


@app.post("/admin/resume/{op_key:path}")
async def resume(op_key: str):
    """手动释放挂起的操作键回执，唤醒所有等待中的请求/重试。"""
    op = devdb.get_operation(op_key)
    if op is None:
        raise HTTPException(status_code=404, detail="操作键不存在")
    released = hold.release(op_key)
    return {"released": released, "op_key": op_key, "receipt": op}


@app.delete("/admin/pause")
async def cancel_pause():
    """撤销尚未触发的一次性暂停臂。"""
    devdb.disarm_pause()
    return {"disarmed": True}


@app.get("/admin/state")
async def admin_state():
    return {
        "pause_arm": devdb.get_pause_arm(),
        "held_in_memory": hold.held_keys_in_memory(),
        "balances": devdb.get_balances(),
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "device-simulator"}

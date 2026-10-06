"""挂起（暂停）状态管理：成功扣量已原子落库，但回执暂不返回，等待手动释放。

进程内以 asyncio.Event 跟踪存活的挂起；设备进程重启后，数据库里遗留的
held=1 记录不再有对应 Event（无法再被手动信号唤醒），重放时按时间戳
识别为“重启前遗留挂起”，直接清除标记并幂等重放回执——扣量事实永不丢失。
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import anyio
from fastapi import Request

from .config import WAIT_POLL_SECONDS
from . import db as devdb

logger = logging.getLogger("device.hold")

# 设备进程启动时刻：held 记录早于该时刻即为重启前遗留
BOOT_AT = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

_events: dict[str, asyncio.Event] = {}
_registry_lock = asyncio.Lock()


async def _event_for(op_key: str) -> asyncio.Event:
    async with _registry_lock:
        event = _events.get(op_key)
        if event is None:
            event = asyncio.Event()
            _events[op_key] = event
        return event


def release(op_key: str) -> bool:
    """手动释放操作键：持久化 held=0 并唤醒所有等待者（含重试请求）。"""
    released = devdb.release_op(op_key)
    event = _events.get(op_key)
    if event is not None:
        event.set()
    logger.info("操作键 %s 已手动释放（released=%s）", op_key, released)
    return released


async def gate(
    op_key: str, hold_state: str | None, created_at: str, request: Request | None
) -> str:
    """挂起门控。hold_state 为 apply_dispense 的返回值。

    返回：
      * "none"    —— 未挂起，直接返回
      * "stale"   —— 重启前遗留挂起，已清除标记，调用方直接重放回执
      * "released"—— 挂起结束（手动释放），调用方重查后返回回执
      * "gone"    —— 调用方已断开
    """
    if hold_state is None:
        return "none"
    if hold_state == "existing_held" and created_at < BOOT_AT:
        # 设备重启前遗留：原等待上下文已不存在，按幂等重放，不阻塞任何人
        if await anyio.to_thread.run_sync(devdb.release_op, op_key):
            logger.warning("操作键 %s 为设备重启前遗留挂起，清除标记并重放回执", op_key)
        return "stale"

    event = await _event_for(op_key)
    logger.warning("操作键 %s 回执挂起，等待手动 /admin/resume 释放（state=%s）", op_key, hold_state)
    while not event.is_set():
        # 兜底：释放可能发生在 Event 注册之前（如重试竞态），轮询数据库标记
        op = await anyio.to_thread.run_sync(devdb.get_operation, op_key)
        if op is not None and not op["held"]:
            event.set()
            return "released"
        try:
            await asyncio.wait_for(event.wait(), timeout=WAIT_POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
        if request is not None and await request.is_disconnected():
            logger.info("调用方已断开，让出挂起等待位 op=%s（重试仍需等待释放）", op_key)
            return "gone"
    return "released"


def held_keys_in_memory() -> list[str]:
    return sorted(k for k, ev in _events.items() if not ev.is_set())

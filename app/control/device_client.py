"""控制端 -> 设备模拟器 HTTP 客户端（异步）。"""
from __future__ import annotations

from typing import Any

import httpx

from .config import DEVICE_TIMEOUT_SECONDS, DEVICE_URL


class DeviceConflictError(Exception):
    """设备返回 409：同操作键但参数不同（理论上属于程序错误，不可重试）。"""


class DeviceUnavailableError(Exception):
    """连接失败 / 超时：可凭同一操作键重试。"""


async def dispense(op_key: str, batch_id: str, channel: str, step_no: int, amount_ml: int) -> dict[str, Any]:
    body = {
        "op_key": op_key,
        "batch_id": batch_id,
        "channel": channel,
        "step_no": step_no,
        "amount_ml": amount_ml,
    }
    async with httpx.AsyncClient(base_url=DEVICE_URL, timeout=DEVICE_TIMEOUT_SECONDS) as client:
        try:
            resp = await client.post("/dispense", json=body)
        except httpx.HTTPError as exc:
            raise DeviceUnavailableError(str(exc)) from exc
        if resp.status_code == 409:
            raise DeviceConflictError(resp.text)
        if resp.status_code >= 400:
            raise DeviceUnavailableError(f"device HTTP {resp.status_code}: {resp.text}")
        return resp.json()


async def get_balances() -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=DEVICE_URL, timeout=3.0) as client:
        try:
            resp = await client.get("/balances")
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError:
            return {"A": None, "B": None, "error": "device unavailable"}

"""批次状态计算：待确认 / 已完成 / 首步失败 / 部分完成（恰好四种）。"""
from __future__ import annotations

from typing import Any

PENDING = "pending"             # 待确认：尚有步骤未确认（含部分步骤已确认、后续执行/接管中）
COMPLETED = "completed"         # 已完成：全部步骤成功
FIRST_STEP_FAILED = "first_step_failed"  # 首步失败：第 1 步即被设备拒绝
PARTIAL = "partial"             # 部分完成：有成功扣量后遇余量不足，后续停止且不回滚

STATUS_TEXT = {
    PENDING: "待确认",
    COMPLETED: "已完成",
    FIRST_STEP_FAILED: "首步失败",
    PARTIAL: "部分完成",
}


def compute_status(batch: dict[str, Any], receipts: list[dict[str, Any]]) -> str:
    total = batch["total_steps"]
    confirmed = len(receipts)
    if confirmed == 0:
        return PENDING
    if confirmed == 1 and receipts[0]["receipt"].get("status") == "rejected":
        return FIRST_STEP_FAILED
    if any(r["receipt"].get("status") == "rejected" for r in receipts):
        return PARTIAL
    if confirmed >= total:
        return COMPLETED
    # 已有成功回执但后续步骤尚未全部确认（执行中 / 等待接管）：仍属待确认
    return PENDING


def summarize(batch: dict[str, Any], receipts: list[dict[str, Any]], balances: dict[str, Any]) -> dict[str, Any]:
    status = compute_status(batch, receipts)
    return {
        "batch_id": batch["batch_id"],
        "request_key": batch["request_key"],
        "channel": batch["channel"],
        "steps": batch["steps"],
        "total_steps": batch["total_steps"],
        "confirmed_steps": len(receipts),
        "created_at": batch["created_at"],
        "status": status,
        "status_text": STATUS_TEXT[status],
        "confirmed_receipts": receipts,
        "device_remaining_ml": balances.get(batch["channel"]),
    }

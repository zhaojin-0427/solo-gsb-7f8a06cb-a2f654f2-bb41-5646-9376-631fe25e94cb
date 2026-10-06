"""异步驱动 worker：每个控制进程各运行一个，协作领取批次步骤。

关键保证：
* 同批次只能按序确认步骤；步骤 N 的回执在控制端落库前绝不发送步骤 N+1；
* 设备操作键 op_key = "{batch_id}:{step_no}"，重试/接管全部复用同一键，
  设备按 (op_key, 参数) 幂等，永远不会重复扣量；
* 进程崩溃 / 重启 / 网络挂起导致租约过期后，另一控制进程凭相同操作键接管。
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from . import db, device_client
from .config import LEASE_TTL_SECONDS, WORKER_ID, WORKER_POLL_SECONDS

logger = logging.getLogger("worker")


def op_key_for(batch_id: str, step_no: int) -> str:
    return f"{batch_id}:{step_no}"


async def _drive_batch(batch: dict[str, Any]) -> bool:
    """尽力推进一个批次；返回是否仍有活可干。设备不可用 / 被暂停时返回 False 让出。"""
    batch_id = batch["batch_id"]
    claim = await asyncio.to_thread(db.claim_next_step, batch_id, WORKER_ID, LEASE_TTL_SECONDS)
    if claim is None:
        return False  # 已终态，或租约在其它存活进程手中

    step_no = claim["step_no"]
    amount = batch["steps"][step_no - 1]
    op_key = op_key_for(batch_id, step_no)

    try:
        receipt: dict[str, Any] = await device_client.dispense(
            op_key=op_key,
            batch_id=batch_id,
            channel=batch["channel"],
            step_no=step_no,
            amount_ml=amount,
        )
    except device_client.DeviceConflictError:
        # 同键不同参数：程序级冲突，绝不落库“错误回执”，等待人工排查
        logger.exception("[%s] 设备报告操作键冲突 op=%s", WORKER_ID, op_key)
        return False
    except device_client.DeviceUnavailableError:
        # 超时 / 连接失败（含暂停期间等待被读超时打断）：租约内下轮重试，
        # 租约过期则由另一个控制进程凭同一 op_key 接管
        return False

    # 防呆：设备回执必须与本次操作键一致
    if receipt.get("op_key") != op_key or receipt.get("batch_id") != batch_id \
            or receipt.get("step_no") != step_no or receipt.get("channel") != batch["channel"]:
        logger.error("[%s] 设备回执与操作键不匹配 op=%s receipt=%s", WORKER_ID, op_key, receipt)
        return False

    try:
        await asyncio.to_thread(db.complete_step, batch_id, step_no, op_key, receipt, WORKER_ID)
    except db.LeaseLostError:
        # 租约已过期并被另一进程接管：放弃本次提交。接管方会凭同一
        # op_key 从设备重放到同一张回执，不存在重复扣量。
        logger.info("[%s] 租约已被接管，放弃提交 op=%s", WORKER_ID, op_key)
        return False
    status = receipt.get("status")
    balance = receipt.get("remaining_ml_after")
    logger.info(
        "[%s] 回执已落库 op=%s status=%s balance_after=%s",
        WORKER_ID, op_key, status, balance,
    )
    return status == "success" and step_no < batch["total_steps"]


async def run_forever() -> None:
    logger.info("worker %s 启动，租约 TTL=%ss", WORKER_ID, LEASE_TTL_SECONDS)
    while True:
        try:
            batches = await asyncio.to_thread(db.list_all_batches)
            did_work = False
            for batch in batches:
                again = await _drive_batch(batch)
                did_work = did_work or again
            if not did_work:
                await asyncio.sleep(WORKER_POLL_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("worker 循环异常")
            await asyncio.sleep(WORKER_POLL_SECONDS)

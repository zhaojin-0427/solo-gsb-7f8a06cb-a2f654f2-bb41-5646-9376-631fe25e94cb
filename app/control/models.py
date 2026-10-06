"""请求模型：非法输入（缺字段、类型错误、步骤数/取值越界等）由 Pydantic 拒绝 -> HTTP 422。"""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

Channel = Literal["A", "B"]


class CreateBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    request_key: str = Field(min_length=1, max_length=200)
    channel: Channel
    steps: list[int] = Field(min_length=1, max_length=3)

    @field_validator("steps")
    @classmethod
    def _validate_steps(cls, v: list[int]) -> list[int]:
        if any(isinstance(x, bool) for x in v):
            raise ValueError("every step must be an integer milliliter amount")
        if any(x <= 0 for x in v):
            raise ValueError("every step must be a positive integer")
        if any(x > 100 for x in v):
            raise ValueError("each step must not exceed 100 mL")
        return v

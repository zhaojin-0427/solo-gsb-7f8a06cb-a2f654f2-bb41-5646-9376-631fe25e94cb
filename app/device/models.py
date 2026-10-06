from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

Channel = Literal["A", "B"]


class DispenseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    op_key: str = Field(min_length=1, max_length=200)
    batch_id: str = Field(min_length=1, max_length=200)
    channel: Channel
    step_no: int = Field(ge=1)
    amount_ml: int = Field(ge=1, le=100)

"""Request models for the device simulator. Invalid input yields HTTP 422."""

from typing import Literal

from pydantic import BaseModel, Field, StrictInt

Channel = Literal["A", "B"]


class DispenseRequest(BaseModel):
    op_key: str = Field(min_length=1, max_length=200)
    batch_id: str = Field(min_length=1, max_length=200)
    step_no: StrictInt = Field(ge=1, le=3)
    channel: Channel
    amount_ml: StrictInt = Field(ge=1, le=100)


class PauseRequest(BaseModel):
    channel: Channel


class ReleaseRequest(BaseModel):
    channel: Channel | None = None

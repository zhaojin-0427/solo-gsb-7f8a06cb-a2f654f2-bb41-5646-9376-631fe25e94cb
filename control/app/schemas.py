"""Request models for the control service. Invalid input yields HTTP 422."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field, StrictInt

Channel = Literal["A", "B"]

# 1..3 sequential steps, each a positive integer of at most 100 mL.
StepList = Annotated[
    list[Annotated[StrictInt, Field(ge=1, le=100)]],
    Field(min_length=1, max_length=3),
]

BatchStatus = Literal["pending", "completed", "failed_first", "partial"]


class BatchCreate(BaseModel):
    request_key: str = Field(min_length=1, max_length=128)
    channel: Channel
    steps: StepList

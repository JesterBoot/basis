from enum import StrEnum

from pydantic import BaseModel


class PipelineOutcome(StrEnum):
    DONE = "done"
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"


class PipelineResult(BaseModel):
    outcome: PipelineOutcome
    reason: str | None = None

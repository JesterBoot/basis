from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel


class HealthState(StrEnum):
    OK = "ok"
    NOT_OK = "not_ok"


class HealthStatus(BaseModel):
    status: HealthState
    time: datetime | None = None

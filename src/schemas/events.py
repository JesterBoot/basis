import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel

from models.enums import EventType


class PaymentCreatedEvent(BaseModel):
    event_id: uuid.UUID
    event_type: Literal[EventType.PAYMENT_CREATED] = EventType.PAYMENT_CREATED
    payment_id: uuid.UUID
    occurred_at: datetime

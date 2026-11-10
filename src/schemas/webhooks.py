import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, field_serializer

from models.enums import Currency, PaymentStatus


class WebhookPayload(BaseModel):
    event_id: uuid.UUID
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None

    @field_serializer("amount")
    def amount_as_string(self, amount: Decimal) -> str:
        return format(amount, "f")

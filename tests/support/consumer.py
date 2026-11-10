import asyncio
import uuid

from core.config import Settings
from models import PaymentStatus
from tests.support.webhooks import WEBHOOK_SECRET


def consumer_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "WEBHOOK_SECRET": WEBHOOK_SECRET,
        "WEBHOOK_ALLOW_PRIVATE": True,
        "WEBHOOK_TIMEOUT": 1,
        "GATEWAY_DELAY_MIN": 0,
        "GATEWAY_DELAY_MAX": 0,
    }
    values.update(overrides)
    return Settings(**values)


class FakeGateway:
    def __init__(
        self, outcome: PaymentStatus = PaymentStatus.SUCCEEDED, delay: float = 0
    ) -> None:
        self.outcome = outcome
        self.delay = delay
        self.charged: list[uuid.UUID] = []

    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        self.charged.append(payment_id)
        if self.delay:
            await asyncio.sleep(self.delay)
        return self.outcome

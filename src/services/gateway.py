import asyncio
import random
import uuid
from collections.abc import Awaitable, Callable
from typing import Protocol

from core.config import Settings, get_settings
from core.custom_logging import get_logger
from models import PaymentStatus

logger = get_logger()

Sleep = Callable[[float], Awaitable[None]]


class PaymentGateway(Protocol):
    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        ...


class MockPaymentGateway:  # Эмулирую задержку и success rate
    def __init__(
        self,
        settings: Settings | None = None,
        rng: random.Random | None = None,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self.settings = settings or get_settings()
        self.rng = rng or random.Random()
        self.sleep = sleep

    async def charge(self, payment_id: uuid.UUID) -> PaymentStatus:
        delay = self.rng.uniform(
            self.settings.gateway_delay_min, self.settings.gateway_delay_max
        )
        await self.sleep(delay)
        approved = self.rng.random() < self.settings.gateway_success_rate

        fields = {"payment_id": str(payment_id), "delay_seconds": round(delay, 3)}
        if approved:
            logger.info("Gateway approved payment", **fields)
            return PaymentStatus.SUCCEEDED
        logger.info("Gateway declined payment", **fields)
        return PaymentStatus.FAILED

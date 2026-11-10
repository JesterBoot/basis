import uuid
from datetime import timedelta
from typing import Any

from sqlalchemy import func, update

from core.config import Settings
from db.session import AsyncSession
from messaging.publisher import EventPublisher
from models import OutboxEvent
from services.outbox import OutboxRelayService


class FakePublisher:
    def __init__(self, fail_on: set[int] | None = None) -> None:
        self.fail_on = fail_on or set()
        self.calls = 0
        self.published: list[uuid.UUID] = []

    async def publish(self, event: OutboxEvent) -> None:
        self.calls += 1
        if self.calls in self.fail_on:
            raise ConnectionError("broker is down")
        self.published.append(event.id)


def relay_service(
    session: AsyncSession, publisher: EventPublisher, batch_size: int
) -> OutboxRelayService:
    settings = Settings(_env_file=None, OUTBOX_BATCH_SIZE=batch_size)
    return OutboxRelayService(session, publisher, settings)


async def add_events(
    session: AsyncSession, count: int, **overrides: Any
) -> list[OutboxEvent]:
    events = [
        OutboxEvent(
            aggregate_type="payment",
            aggregate_id=uuid.uuid4(),
            event_type="payment.created",
            routing_key="payments.new",
            payload={"n": n},
            **overrides,
        )
        for n in range(count)
    ]
    session.add_all(events)
    await session.commit()
    for offset, event in enumerate(events):
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event.id)
            .values(next_attempt_at=func.now() - timedelta(seconds=count - offset))
        )
    await session.commit()
    return events

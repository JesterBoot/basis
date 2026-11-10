import asyncio
import json
import uuid

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker
from sqlalchemy import select

from core.config import Settings
from db.session import AsyncSession
from messaging.names import Queue
from messaging.publisher import EventPublisher, RabbitEventPublisher
from models import OutboxEvent
from schemas.payment import PaymentCreate
from services.outbox import OutboxRelayService
from services.payments import PaymentService

pytestmark = [pytest.mark.database, pytest.mark.rabbitmq]


def relay_service(
    session: AsyncSession, publisher: EventPublisher, batch_size: int
) -> OutboxRelayService:
    settings = Settings(_env_file=None, OUTBOX_BATCH_SIZE=batch_size)
    return OutboxRelayService(session, publisher, settings)


BODY = {
    "amount": "42.00",
    "currency": "USD",
    "description": "relay integration",
    "webhook_url": "https://merchant.example/hooks",
}


async def next_message(
    channel: aio_pika.abc.AbstractChannel, queue_name: str
) -> aio_pika.abc.AbstractIncomingMessage | None:
    queue = await channel.get_queue(queue_name)
    for _ in range(60):
        message = await queue.get(no_ack=False, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.05)
    return None


async def test_created_payment_reaches_payments_new_queue(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    created = await PaymentService(session).create(
        PaymentCreate.model_validate(BODY), f"relay-{uuid.uuid4()}"
    )
    publisher = RabbitEventPublisher(broker, timeout=5)

    result = await relay_service(session, publisher, 10).relay_batch()
    message = await next_message(amqp_channel, Queue.PAYMENTS_NEW)

    assert result.published == 1
    assert message is not None
    event = await session.scalar(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id == created.payment.payment_id
        )
    )
    assert event is not None
    assert message.message_id == str(event.id)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    assert message.content_type == "application/json"
    assert message.type == "payment.created"
    assert message.headers == {"x-attempt": 1}
    assert json.loads(message.body) == event.payload
    assert json.loads(message.body)["payment_id"] == str(created.payment.payment_id)
    await message.ack()


async def test_unroutable_event_is_rescheduled_not_marked_published(
    session: AsyncSession, broker: RabbitBroker
) -> None:
    event = OutboxEvent(
        aggregate_type="payment",
        aggregate_id=uuid.uuid4(),
        event_type="payment.created",
        routing_key="nowhere",
        payload={"lost": True},
    )
    session.add(event)
    await session.commit()

    result = await relay_service(
        session, RabbitEventPublisher(broker, timeout=5), 10
    ).relay_batch()

    stored = await session.get(OutboxEvent, event.id, populate_existing=True)
    assert result.failed == 1
    assert stored is not None
    assert stored.published_at is None
    assert stored.attempts == 1
    assert stored.last_error is not None
    assert "NO_ROUTE" in stored.last_error

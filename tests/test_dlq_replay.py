import json
import uuid

import aio_pika
import pytest
from faststream.rabbit import RabbitBroker

from db.session import AsyncSession
from messaging.names import Exchange, Queue, RoutingKey
from models import Payment, PaymentStatus, WebhookStatus
from schemas.payment import PaymentCreate
from services.payments import PaymentService
from services.processing import PaymentProcessingService
from tests.support.rabbitmq import count_messages
from tools.dlq_replay import DeadLetterReplayer

pytestmark = [pytest.mark.database, pytest.mark.rabbitmq]

BODY = {
    "amount": "7.00",
    "currency": "EUR",
    "description": "dlq replay test",
    "webhook_url": "https://merchant.example/hooks",
}


async def failed_webhook_payment(session: AsyncSession) -> tuple[uuid.UUID, bytes]:
    created = await PaymentService(session).create(
        PaymentCreate.model_validate(BODY), f"dlq-{uuid.uuid4()}"
    )
    payment_id = created.payment.payment_id
    await PaymentProcessingService(session).complete(
        payment_id, PaymentStatus.SUCCEEDED
    )
    payment = await session.get(Payment, payment_id)
    assert payment is not None
    payment.webhook_status = WebhookStatus.FAILED
    payment.webhook_attempts = 3
    await session.commit()
    body = json.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "payment.created",
            "payment_id": str(payment_id),
            "occurred_at": "2026-11-10T12:00:00Z",
        }
    ).encode()
    return payment_id, body


async def dead_letter(broker: RabbitBroker, body: bytes, message_id: str) -> None:
    await broker.publish(
        body,
        exchange=Exchange.PAYMENTS_DLX,
        routing_key=RoutingKey.PAYMENTS_DLQ,
        message_id=message_id,
        message_type="payment.created",
        content_type="application/json",
        headers={"x-attempt": 3},
        persist=True,
    )


async def test_replay_moves_message_back_and_reopens_webhook(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    payment_id, body = await failed_webhook_payment(session)
    await dead_letter(broker, body, "msg-1")

    summary = await DeadLetterReplayer(amqp_channel).replay(limit=10)

    assert summary.model_dump() == {
        "replayed": 1,
        "reopened_webhooks": 1,
        "skipped_invalid": 0,
    }
    assert await count_messages(amqp_channel, Queue.PAYMENTS_DLQ) == 0
    queue = await amqp_channel.get_queue(Queue.PAYMENTS_NEW)
    replayed = await queue.get(no_ack=True, fail=False)
    assert replayed is not None
    assert replayed.body == body
    assert replayed.message_id == "msg-1"
    assert replayed.headers == {"x-attempt": 1}
    assert replayed.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    assert payment.webhook_status is WebhookStatus.PENDING
    assert payment.webhook_attempts == 3
    assert payment.status is PaymentStatus.SUCCEEDED


async def test_invalid_messages_stay_in_dead_letter_queue(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    _, body = await failed_webhook_payment(session)
    await dead_letter(broker, b"garbage", "bad-1")
    await dead_letter(broker, body, "good-1")

    summary = await DeadLetterReplayer(amqp_channel).replay(limit=10)

    assert (summary.replayed, summary.skipped_invalid) == (1, 1)
    assert await count_messages(amqp_channel, Queue.PAYMENTS_DLQ) == 1
    assert await count_messages(amqp_channel, Queue.PAYMENTS_NEW) == 1


async def test_limit_is_respected(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    for n in range(3):
        _, body = await failed_webhook_payment(session)
        await dead_letter(broker, body, f"msg-{n}")

    summary = await DeadLetterReplayer(amqp_channel).replay(limit=2)

    assert summary.replayed == 2
    assert await count_messages(amqp_channel, Queue.PAYMENTS_DLQ) == 1


async def test_dry_run_lists_without_moving(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    payment_id, body = await failed_webhook_payment(session)
    await dead_letter(broker, body, "msg-1")
    await dead_letter(broker, b"garbage", "bad-1")

    listed = await DeadLetterReplayer(amqp_channel).peek(limit=10)

    assert [(item.message_id, item.valid) for item in listed] == [
        ("msg-1", True),
        ("bad-1", False),
    ]
    assert listed[0].payment_id == str(payment_id)
    assert listed[0].attempt == 3
    assert await count_messages(amqp_channel, Queue.PAYMENTS_DLQ) == 2
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    assert payment.webhook_status is WebhookStatus.FAILED


async def test_empty_dead_letter_queue_is_a_no_op(
    amqp_channel: aio_pika.abc.AbstractChannel, broker: RabbitBroker
) -> None:
    summary = await DeadLetterReplayer(amqp_channel).replay(limit=10)

    assert summary.replayed == 0
    assert await DeadLetterReplayer(amqp_channel).peek(limit=10) == []

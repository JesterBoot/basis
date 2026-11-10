import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import aio_pika
import httpx
import pytest
import respx
from faststream.rabbit import RabbitBroker
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from core.config import Settings
from db.session import AsyncSession
from messaging.names import Exchange, Queue, RoutingKey
from messaging.publisher import RabbitEventPublisher
from models import Payment, PaymentStatus, WebhookStatus
from schemas.payment import PaymentCreate
from services.outbox import OutboxRelayService
from services.payments import PaymentService
from services.pipeline import PaymentPipeline
from services.webhooks import WebhookClient
from tests.support.consumer import WEBHOOK_SECRET, WEBHOOK_URL, FakeGateway
from workers.consumer import create_consumer

pytestmark = [pytest.mark.database, pytest.mark.rabbitmq]

BODY = {
    "amount": "15.00",
    "currency": "USD",
    "description": "consumer test",
    "webhook_url": WEBHOOK_URL,
}


@pytest.fixture
def settings(rabbitmq_settings: Settings) -> Settings:
    return rabbitmq_settings.model_copy(
        update={
            "webhook_secret": SecretStr(WEBHOOK_SECRET),
            "webhook_allow_private": True,
            "webhook_timeout": 1,
            "consumer_prefetch": 10,
        }
    )


@asynccontextmanager
async def running_consumer(
    settings: Settings, gateway: FakeGateway
) -> AsyncIterator[RabbitBroker]:
    async with WebhookClient.from_settings(settings) as webhook_client:
        consumer = create_consumer(
            settings, PaymentPipeline(gateway, webhook_client, settings)
        )
        await consumer.start()
        try:
            yield consumer
        finally:
            await consumer.stop()


async def create_and_relay(
    session: AsyncSession, broker: RabbitBroker, settings: Settings, count: int = 1
) -> list[uuid.UUID]:
    service = PaymentService(session)
    payment_ids = [
        (
            await service.create(
                PaymentCreate.model_validate(BODY), f"consumer-{uuid.uuid4()}"
            )
        ).payment.payment_id
        for _ in range(count)
    ]
    relay = OutboxRelayService(
        session, RabbitEventPublisher(broker, timeout=5), settings
    )
    result = await relay.relay_batch()
    assert result.published == count
    return payment_ids


async def wait_for(
    session: AsyncSession,
    payment_ids: list[uuid.UUID],
    done: Callable[[Payment], bool],
    timeout: float = 10,
) -> list[Payment]:
    deadline = time.monotonic() + timeout
    while True:
        payments = [
            await session.get(Payment, payment_id, populate_existing=True)
            for payment_id in payment_ids
        ]
        if all(payment is not None and done(payment) for payment in payments):
            return [payment for payment in payments if payment is not None]
        if time.monotonic() > deadline:
            raise AssertionError(f"Timed out waiting for {payment_ids}")
        await asyncio.sleep(0.05)


async def queue_depth(channel: aio_pika.abc.AbstractChannel, name: str) -> int:
    queue = await channel.declare_queue(name, passive=True)
    return queue.declaration_result.message_count or 0


async def test_payment_flows_from_outbox_to_webhook(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)

    async with running_consumer(settings, gateway):
        (payment_id,) = await create_and_relay(session, broker, settings)
        (payment,) = await wait_for(
            session,
            [payment_id],
            lambda p: p.webhook_status is WebhookStatus.DELIVERED,
        )

    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.processed_at is not None
    assert gateway.charged == [payment_id]
    assert route.call_count == 1
    assert await queue_depth(amqp_channel, Queue.PAYMENTS_NEW) == 0
    assert await queue_depth(amqp_channel, Queue.PAYMENTS_DLQ) == 0


async def test_poison_message_goes_to_dead_letter_queue(
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
) -> None:
    gateway = FakeGateway()

    async with running_consumer(settings, gateway):
        await broker.publish(
            b"definitely not json",
            exchange=Exchange.PAYMENTS,
            routing_key=RoutingKey.PAYMENTS_NEW,
        )
        dlq = await amqp_channel.get_queue(Queue.PAYMENTS_DLQ)
        dead = None
        for _ in range(100):
            dead = await dlq.get(no_ack=True, fail=False)
            if dead is not None:
                break
            await asyncio.sleep(0.05)

    assert dead is not None
    assert dead.body == b"definitely not json"
    assert dead.headers["x-death"][0]["reason"] == "rejected"
    assert gateway.charged == []


async def test_payments_are_processed_concurrently(
    session: AsyncSession,
    broker: RabbitBroker,
    settings: Settings,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, delay=0.5)

    async with running_consumer(settings, gateway):
        started = time.monotonic()
        payment_ids = await create_and_relay(session, broker, settings, count=10)
        await wait_for(
            session,
            payment_ids,
            lambda p: p.webhook_status is WebhookStatus.DELIVERED,
        )
        elapsed = time.monotonic() - started

    assert sorted(gateway.charged) == sorted(payment_ids)
    assert elapsed < 2.5


async def next_dead_letter(
    channel: aio_pika.abc.AbstractChannel, timeout: float = 10
) -> aio_pika.abc.AbstractIncomingMessage:
    dlq = await channel.get_queue(Queue.PAYMENTS_DLQ)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = await dlq.get(no_ack=True, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.05)
    raise AssertionError("No message reached the dead letter queue")


async def test_failing_webhook_is_retried_then_dead_lettered(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(503))
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)

    async with running_consumer(settings, gateway):
        started = time.monotonic()
        (payment_id,) = await create_and_relay(session, broker, settings)
        dead = await next_dead_letter(amqp_channel)
        elapsed = time.monotonic() - started
        (payment,) = await wait_for(
            session, [payment_id], lambda p: p.webhook_status is WebhookStatus.FAILED
        )

    assert route.call_count == 3
    assert gateway.charged == [payment_id]
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_attempts == 3
    assert payment.webhook_last_error == "HTTP 503"
    assert dead.headers["x-attempt"] == 3
    assert dead.headers["x-death"][0]["reason"] == "rejected"
    assert dead.message_id is not None
    assert elapsed >= 0.9
    for queue in (
        Queue.PAYMENTS_NEW,
        "payments.new.retry.300ms",
        "payments.new.retry.600ms",
    ):
        assert await queue_depth(amqp_channel, queue) == 0


async def test_webhook_recovers_on_retry(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200)]
    )

    async with running_consumer(settings, FakeGateway()):
        (payment_id,) = await create_and_relay(session, broker, settings)
        (payment,) = await wait_for(
            session,
            [payment_id],
            lambda p: p.webhook_status is WebhookStatus.DELIVERED,
        )

    assert route.call_count == 2
    assert payment.webhook_attempts == 2
    assert payment.webhook_last_error is None
    assert await queue_depth(amqp_channel, Queue.PAYMENTS_DLQ) == 0


class DatabaseDownPipeline:
    def __init__(self) -> None:
        self.calls = 0

    async def process(self, body: bytes) -> object:
        self.calls += 1
        raise OperationalError("SELECT 1", {}, Exception("database is down"))

    async def give_up(self, payment_id: uuid.UUID) -> None:
        raise AssertionError("must not give up on a payment it never loaded")


async def test_database_outage_exhausts_retries_and_keeps_payment_pending(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
) -> None:
    pipeline = DatabaseDownPipeline()
    consumer = create_consumer(settings, pipeline)  # type: ignore[arg-type]
    await consumer.start()
    try:
        (payment_id,) = await create_and_relay(session, broker, settings)
        dead = await next_dead_letter(amqp_channel)
    finally:
        await consumer.stop()

    assert pipeline.calls == 3
    assert dead.headers["x-attempt"] == 3
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    assert payment.status is PaymentStatus.PENDING
    assert payment.webhook_status is WebhookStatus.PENDING


async def wait_until(predicate: Callable[[], bool], timeout: float = 5) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("Condition not met in time")
        await asyncio.sleep(0.02)


async def test_graceful_stop_finishes_in_flight_payment(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    gateway = FakeGateway(PaymentStatus.SUCCEEDED, delay=1.0)
    graceful = settings.model_copy(update={"consumer_graceful_timeout": 5})

    async with WebhookClient.from_settings(graceful) as webhook_client:
        consumer = create_consumer(
            graceful, PaymentPipeline(gateway, webhook_client, graceful)
        )
        await consumer.start()
        (payment_id,) = await create_and_relay(session, broker, graceful)
        await wait_until(lambda: bool(gateway.charged))
        await consumer.stop()

    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    assert payment.status is PaymentStatus.SUCCEEDED
    assert payment.webhook_status is WebhookStatus.DELIVERED
    assert await queue_depth(amqp_channel, Queue.PAYMENTS_NEW) == 0


async def test_unfinished_message_is_redelivered_and_processed_once(
    session: AsyncSession,
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    settings: Settings,
    respx_mock: respx.MockRouter,
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    slow_gateway = FakeGateway(PaymentStatus.FAILED, delay=3.0)
    impatient = settings.model_copy(update={"consumer_graceful_timeout": 0.2})

    async with WebhookClient.from_settings(impatient) as webhook_client:
        first = create_consumer(
            impatient, PaymentPipeline(slow_gateway, webhook_client, impatient)
        )
        await first.start()
        (payment_id,) = await create_and_relay(session, broker, impatient)
        await wait_until(lambda: bool(slow_gateway.charged))
        await first.stop()

    still_pending = await session.get(Payment, payment_id, populate_existing=True)
    assert still_pending is not None
    assert still_pending.status is PaymentStatus.PENDING

    fast_gateway = FakeGateway(PaymentStatus.SUCCEEDED)
    async with running_consumer(settings, fast_gateway):
        (payment,) = await wait_for(
            session,
            [payment_id],
            lambda p: p.webhook_status is WebhookStatus.DELIVERED,
        )
        await asyncio.sleep(3.2)

    final = await session.get(Payment, payment_id, populate_existing=True)
    assert final is not None
    assert payment.status is PaymentStatus.SUCCEEDED
    assert final.status is PaymentStatus.SUCCEEDED
    assert fast_gateway.charged == [payment_id]
    assert route.call_count == 1
    assert await queue_depth(amqp_channel, Queue.PAYMENTS_NEW) == 0

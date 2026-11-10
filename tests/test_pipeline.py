import json
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio
import respx
from sqlalchemy import select

from db.session import AsyncSession
from models import OutboxEvent, Payment, PaymentStatus, WebhookStatus
from schemas.payment import PaymentCreate
from schemas.pipeline import PipelineOutcome
from services.payments import PaymentService
from services.pipeline import PaymentPipeline
from services.processing import PaymentProcessingService
from services.webhooks import WebhookClient
from tests.support.consumer import WEBHOOK_URL, FakeGateway, consumer_settings

pytestmark = pytest.mark.database

BODY = {
    "amount": "25.00",
    "currency": "RUB",
    "description": "pipeline test",
    "webhook_url": WEBHOOK_URL,
}


@pytest_asyncio.fixture
async def webhook_client() -> AsyncIterator[WebhookClient]:
    async with WebhookClient.from_settings(consumer_settings()) as client:
        yield client


async def create_event(session: AsyncSession) -> tuple[uuid.UUID, bytes]:
    created = await PaymentService(session).create(
        PaymentCreate.model_validate(BODY), f"pipe-{uuid.uuid4()}"
    )
    event = await session.scalar(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id == created.payment.payment_id
        )
    )
    assert event is not None
    return created.payment.payment_id, json.dumps(event.payload).encode()


async def reload(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    return payment


def pipeline(gateway: FakeGateway, client: WebhookClient) -> PaymentPipeline:
    return PaymentPipeline(gateway, client, consumer_settings())


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_new_payment_is_charged_and_notified(
    session: AsyncSession,
    webhook_client: WebhookClient,
    respx_mock: respx.MockRouter,
    outcome: PaymentStatus,
) -> None:
    payment_id, body = await create_event(session)
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    gateway = FakeGateway(outcome)

    result = await pipeline(gateway, webhook_client).process(body)

    assert result.outcome is PipelineOutcome.DONE
    assert gateway.charged == [payment_id]
    stored = await reload(session, payment_id)
    assert stored.status is outcome
    assert stored.webhook_status is WebhookStatus.DELIVERED
    sent = json.loads(route.calls.last.request.content)
    assert sent["payment_id"] == str(payment_id)
    assert sent["status"] == outcome.value


async def test_duplicate_message_does_nothing(
    session: AsyncSession, webhook_client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    _, body = await create_event(session)
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    gateway = FakeGateway()
    worker = pipeline(gateway, webhook_client)
    await worker.process(body)

    result = await worker.process(body)

    assert result.outcome is PipelineOutcome.DONE
    assert len(gateway.charged) == 1
    assert route.call_count == 1


async def test_redelivery_after_status_update_only_sends_webhook(
    session: AsyncSession, webhook_client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment_id, body = await create_event(session)
    await PaymentProcessingService(session).complete(payment_id, PaymentStatus.FAILED)
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(204))
    gateway = FakeGateway(PaymentStatus.SUCCEEDED)

    result = await pipeline(gateway, webhook_client).process(body)

    assert result.outcome is PipelineOutcome.DONE
    assert gateway.charged == []
    assert json.loads(route.calls.last.request.content)["status"] == "failed"
    stored = await reload(session, payment_id)
    assert stored.status is PaymentStatus.FAILED
    assert stored.webhook_status is WebhookStatus.DELIVERED


async def test_transient_webhook_failure_asks_for_retry(
    session: AsyncSession, webhook_client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment_id, body = await create_event(session)
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(503))

    result = await pipeline(FakeGateway(), webhook_client).process(body)

    assert result.outcome is PipelineOutcome.RETRY
    assert result.reason == "HTTP 503"
    stored = await reload(session, payment_id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_status is WebhookStatus.PENDING
    assert stored.webhook_attempts == 1


async def test_retry_after_transient_failure_skips_gateway(
    session: AsyncSession, webhook_client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    _, body = await create_event(session)
    respx_mock.post(WEBHOOK_URL).mock(
        side_effect=[httpx.Response(500), httpx.Response(200)]
    )
    gateway = FakeGateway()
    worker = pipeline(gateway, webhook_client)

    first = await worker.process(body)
    second = await worker.process(body)

    assert (first.outcome, second.outcome) == (
        PipelineOutcome.RETRY,
        PipelineOutcome.DONE,
    )
    assert len(gateway.charged) == 1


async def test_permanent_webhook_rejection_is_done(
    session: AsyncSession, webhook_client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment_id, body = await create_event(session)
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(404))
    worker = pipeline(FakeGateway(), webhook_client)

    result = await worker.process(body)
    again = await worker.process(body)

    assert result.outcome is PipelineOutcome.DONE
    assert result.reason == "HTTP 404"
    assert again.outcome is PipelineOutcome.DONE
    stored = await reload(session, payment_id)
    assert stored.webhook_status is WebhookStatus.FAILED
    assert stored.webhook_attempts == 1


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"{}",
        b'{"event_id": "x", "payment_id": "y"}',
        json.dumps(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": "payment.updated",
                "payment_id": str(uuid.uuid4()),
                "occurred_at": "2026-11-10T12:00:00Z",
            }
        ).encode(),
    ],
    ids=["garbage", "empty", "bad-uuids", "wrong-type"],
)
async def test_invalid_messages_are_dead_lettered(
    session: AsyncSession, webhook_client: WebhookClient, body: bytes
) -> None:
    gateway = FakeGateway()

    result = await pipeline(gateway, webhook_client).process(body)

    assert result.outcome is PipelineOutcome.DEAD_LETTER
    assert gateway.charged == []


async def test_unknown_payment_is_dead_lettered(
    session: AsyncSession, webhook_client: WebhookClient
) -> None:
    body = json.dumps(
        {
            "event_id": str(uuid.uuid4()),
            "event_type": "payment.created",
            "payment_id": str(uuid.uuid4()),
            "occurred_at": "2026-11-10T12:00:00Z",
        }
    ).encode()

    result = await pipeline(FakeGateway(), webhook_client).process(body)

    assert result.outcome is PipelineOutcome.DEAD_LETTER
    assert result.reason is not None
    assert "does not exist" in result.reason

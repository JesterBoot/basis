import json
import uuid

import httpx
import pytest
import respx

from db.session import AsyncSession
from models import Payment, PaymentStatus, WebhookStatus
from schemas.payment import PaymentCreate, PaymentRead
from services.exceptions import WebhookPermanentError, WebhookTransientError
from services.payments import PaymentService
from services.processing import PaymentProcessingService
from services.webhooks import WebhookClient, WebhookService
from tests.support.webhooks import WEBHOOK_URL, public_resolver, webhook_settings

pytestmark = pytest.mark.database

BODY = {
    "amount": "100.50",
    "currency": "USD",
    "description": "webhook test",
    "webhook_url": WEBHOOK_URL,
}


async def processed_payment(session: AsyncSession) -> Payment:
    created = await PaymentService(session).create(
        PaymentCreate.model_validate(BODY), f"wh-{uuid.uuid4()}"
    )
    transition = await PaymentProcessingService(session).complete(
        created.payment.payment_id, PaymentStatus.SUCCEEDED
    )
    payment = await session.get(Payment, transition.payment.payment_id)
    assert payment is not None
    return payment


async def reload(session: AsyncSession, payment_id: uuid.UUID) -> Payment:
    payment = await session.get(Payment, payment_id, populate_existing=True)
    assert payment is not None
    return payment


def webhook_service(session: AsyncSession, client: WebhookClient) -> WebhookService:
    return WebhookService(session, client, webhook_settings())


def read(payment: Payment) -> PaymentRead:
    return PaymentRead.model_validate(payment)


async def test_delivery_marks_payment_delivered(
    session: AsyncSession,
    webhook_client: WebhookClient,
    respx_mock: respx.MockRouter,
) -> None:
    payment = await processed_payment(session)
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    event_id = uuid.uuid4()

    await webhook_service(session, webhook_client).deliver(read(payment), event_id)

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.DELIVERED
    assert stored.webhook_attempts == 1
    assert stored.webhook_delivered_at is not None
    assert stored.webhook_last_error is None
    body = json.loads(route.calls.last.request.content)
    assert body["payment_id"] == str(payment.id)
    assert body["event_id"] == str(event_id)
    assert body["status"] == "succeeded"
    assert body["processed_at"] is not None


async def test_transient_failure_records_attempt_and_keeps_pending(
    session: AsyncSession,
    webhook_client: WebhookClient,
    respx_mock: respx.MockRouter,
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(503))
    service = webhook_service(session, webhook_client)

    with pytest.raises(WebhookTransientError):
        await service.deliver(read(payment), uuid.uuid4())
    with pytest.raises(WebhookTransientError):
        await service.deliver(read(payment), uuid.uuid4())

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.PENDING
    assert stored.webhook_attempts == 2
    assert stored.webhook_last_error == "HTTP 503"
    assert stored.status is PaymentStatus.SUCCEEDED


async def test_permanent_failure_marks_webhook_failed(
    session: AsyncSession,
    webhook_client: WebhookClient,
    respx_mock: respx.MockRouter,
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(410))

    with pytest.raises(WebhookPermanentError):
        await webhook_service(session, webhook_client).deliver(
            read(payment), uuid.uuid4()
        )

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.FAILED
    assert stored.webhook_attempts == 1
    assert stored.webhook_last_error == "HTTP 410"
    assert stored.status is PaymentStatus.SUCCEEDED


async def test_success_after_failures_clears_last_error(
    session: AsyncSession,
    webhook_client: WebhookClient,
    respx_mock: respx.MockRouter,
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(WEBHOOK_URL).mock(
        side_effect=[httpx.Response(500), httpx.Response(200)]
    )
    service = webhook_service(session, webhook_client)

    with pytest.raises(WebhookTransientError):
        await service.deliver(read(payment), uuid.uuid4())
    await service.deliver(read(payment), uuid.uuid4())

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.DELIVERED
    assert stored.webhook_attempts == 2
    assert stored.webhook_last_error is None


async def test_long_errors_are_truncated(
    session: AsyncSession,
    webhook_http: httpx.AsyncClient,
    respx_mock: respx.MockRouter,
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(WEBHOOK_URL).mock(side_effect=httpx.ConnectError("x" * 5000))
    settings = webhook_settings(WEBHOOK_LAST_ERROR_MAX_LENGTH=50)
    client = WebhookClient(webhook_http, settings, resolver=public_resolver)

    with pytest.raises(WebhookTransientError):
        await WebhookService(session, client, settings).deliver(
            read(payment), uuid.uuid4()
        )

    stored = await reload(session, payment.id)
    assert stored.webhook_last_error is not None
    assert len(stored.webhook_last_error) <= 50


async def test_mark_failed_does_not_override_delivered(
    session: AsyncSession,
    webhook_client: WebhookClient,
    respx_mock: respx.MockRouter,
) -> None:
    delivered = await processed_payment(session)
    pending = await processed_payment(session)
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    service = webhook_service(session, webhook_client)
    await service.deliver(read(delivered), uuid.uuid4())

    await service.mark_failed(delivered.id)
    await service.mark_failed(pending.id)

    assert (
        await reload(session, delivered.id)
    ).webhook_status is WebhookStatus.DELIVERED
    assert (await reload(session, pending.id)).webhook_status is WebhookStatus.FAILED

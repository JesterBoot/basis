import ipaddress
import json
import socket
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal

import httpx
import pytest
import pytest_asyncio
import respx

from core.config import Settings
from db.session import AsyncSession
from models import Currency, Payment, PaymentStatus, WebhookStatus
from schemas.payment import PaymentCreate, PaymentRead
from schemas.webhooks import WebhookPayload
from services.exceptions import WebhookPermanentError, WebhookTransientError
from services.payments import PaymentService
from services.processing import PaymentProcessingService
from services.webhooks import WebhookClient, WebhookService
from utils.network import IPAddress
from utils.signing import sign, verify

HOOK_URL = "https://merchant.example/hooks/payments"
SECRET = "test-webhook-secret"


def webhook_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "WEBHOOK_SECRET": SECRET,
        "WEBHOOK_ALLOW_PRIVATE": False,
        "WEBHOOK_TIMEOUT": 1,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


async def public_resolver(host: str, port: int) -> list[IPAddress]:
    return [ipaddress.ip_address("93.184.216.34")]


def make_payload(**overrides: object) -> WebhookPayload:
    values: dict[str, object] = {
        "event_id": uuid.uuid4(),
        "payment_id": uuid.uuid4(),
        "status": PaymentStatus.SUCCEEDED,
        "amount": Decimal("100.50"),
        "currency": Currency.USD,
        "processed_at": None,
    }
    values.update(overrides)
    return WebhookPayload.model_validate(values)


@pytest_asyncio.fixture
async def http_client() -> AsyncIterator[httpx.AsyncClient]:
    async with WebhookClient.from_settings(webhook_settings()) as webhook_client:
        yield webhook_client.http


@pytest.fixture
def client(http_client: httpx.AsyncClient) -> WebhookClient:
    return WebhookClient(http_client, webhook_settings(), resolver=public_resolver)


def test_signature_round_trip() -> None:
    body = b'{"a":1}'
    signature = sign(SECRET, body)

    assert signature.startswith("sha256=")
    assert verify(SECRET, body, signature) is True
    assert verify(SECRET, body + b" ", signature) is False
    assert verify("other-secret", body, signature) is False


def test_payload_serializes_amount_as_string() -> None:
    payload = make_payload(amount=Decimal("10.00"))

    assert json.loads(payload.model_dump_json())["amount"] == "10.00"


@pytest.mark.parametrize("secret", [None, "", "   "])
def test_client_requires_a_secret(
    http_client: httpx.AsyncClient, secret: str | None
) -> None:
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRET"):
        WebhookClient(http_client, webhook_settings(WEBHOOK_SECRET=secret))


async def test_request_is_signed_and_has_headers(
    client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(200))
    payload = make_payload()

    await client.send(HOOK_URL, payload)

    request = route.calls.last.request
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["X-Event-Id"] == str(payload.event_id)
    assert request.headers["User-Agent"] == "basis-payments-webhook/1.0"
    assert verify(SECRET, request.content, request.headers["X-Webhook-Signature"])
    assert json.loads(request.content) == {
        "event_id": str(payload.event_id),
        "payment_id": str(payload.payment_id),
        "status": "succeeded",
        "amount": "100.50",
        "currency": "USD",
        "processed_at": None,
    }


@pytest.mark.parametrize("status_code", [200, 201, 202, 204])
async def test_2xx_is_success(
    client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(status_code))

    await client.send(HOOK_URL, make_payload())


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
async def test_retryable_statuses_are_transient(
    client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(status_code))

    with pytest.raises(WebhookTransientError, match=f"HTTP {status_code}"):
        await client.send(HOOK_URL, make_payload())


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 410, 422])
async def test_client_errors_are_permanent(
    client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(status_code))

    with pytest.raises(WebhookPermanentError, match=f"HTTP {status_code}"):
        await client.send(HOOK_URL, make_payload())


@pytest.mark.parametrize("status_code", [301, 302, 307, 308])
async def test_redirects_are_permanent_and_not_followed(
    client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(HOOK_URL).mock(
        return_value=httpx.Response(
            status_code, headers={"Location": "http://10.0.0.1/"}
        )
    )
    internal = respx_mock.post("http://10.0.0.1/")

    with pytest.raises(WebhookPermanentError, match=f"HTTP {status_code}"):
        await client.send(HOOK_URL, make_payload())

    assert not internal.called


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadTimeout("slow"),
        httpx.ConnectTimeout("slow"),
        httpx.ConnectError("refused"),
        httpx.RemoteProtocolError("reset"),
    ],
)
async def test_network_failures_are_transient(
    client: WebhookClient, respx_mock: respx.MockRouter, error: Exception
) -> None:
    respx_mock.post(HOOK_URL).mock(side_effect=error)

    with pytest.raises(WebhookTransientError):
        await client.send(HOOK_URL, make_payload())


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1/hook", "http://localhost:9000/hook", "http://[::1]/hook"],
)
async def test_private_literal_hosts_are_blocked_without_request(
    client: WebhookClient, respx_mock: respx.MockRouter, url: str
) -> None:
    route = respx_mock.post(url)

    with pytest.raises(WebhookPermanentError, match="private"):
        await client.send(url, make_payload())

    assert not route.called


async def test_public_name_resolving_to_private_ip_is_blocked(
    http_client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    async def rebinding_resolver(host: str, port: int) -> list[IPAddress]:
        return [ipaddress.ip_address("8.8.8.8"), ipaddress.ip_address("10.0.0.7")]

    client = WebhookClient(http_client, webhook_settings(), resolver=rebinding_resolver)
    route = respx_mock.post(HOOK_URL)

    with pytest.raises(
        WebhookPermanentError, match="resolves to private address 10.0.0.7"
    ):
        await client.send(HOOK_URL, make_payload())

    assert not route.called


async def test_dns_failure_is_transient(http_client: httpx.AsyncClient) -> None:
    async def failing_resolver(host: str, port: int) -> list[IPAddress]:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    client = WebhookClient(http_client, webhook_settings(), resolver=failing_resolver)

    with pytest.raises(WebhookTransientError, match="DNS lookup failed"):
        await client.send(HOOK_URL, make_payload())


async def test_private_destinations_allowed_when_configured(
    http_client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    settings = webhook_settings(WEBHOOK_ALLOW_PRIVATE=True)

    async def unused_resolver(host: str, port: int) -> list[IPAddress]:
        raise AssertionError("resolver must not be called")

    client = WebhookClient(http_client, settings, resolver=unused_resolver)
    route = respx_mock.post("http://webhook-sink:9000/hook").mock(
        return_value=httpx.Response(204)
    )

    await client.send("http://webhook-sink:9000/hook", make_payload())

    assert route.called


async def test_resolver_receives_default_port(http_client: httpx.AsyncClient) -> None:
    seen: list[tuple[str, int]] = []

    async def recording_resolver(host: str, port: int) -> list[IPAddress]:
        seen.append((host, port))
        return [ipaddress.ip_address("93.184.216.34")]

    client = WebhookClient(http_client, webhook_settings(), resolver=recording_resolver)
    with respx.mock:
        respx.post(HOOK_URL).mock(return_value=httpx.Response(200))
        respx.post("http://merchant.example:8443/hook").mock(
            return_value=httpx.Response(200)
        )
        await client.send(HOOK_URL, make_payload())
        await client.send("http://merchant.example:8443/hook", make_payload())

    assert seen == [("merchant.example", 443), ("merchant.example", 8443)]


BODY = {
    "amount": "100.50",
    "currency": "USD",
    "description": "webhook test",
    "webhook_url": HOOK_URL,
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


@pytest.mark.database
async def test_delivery_marks_payment_delivered(
    session: AsyncSession, client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment = await processed_payment(session)
    route = respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(200))
    event_id = uuid.uuid4()

    await webhook_service(session, client).deliver(read(payment), event_id)

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


@pytest.mark.database
async def test_transient_failure_records_attempt_and_keeps_pending(
    session: AsyncSession, client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(503))
    service = webhook_service(session, client)

    with pytest.raises(WebhookTransientError):
        await service.deliver(read(payment), uuid.uuid4())
    with pytest.raises(WebhookTransientError):
        await service.deliver(read(payment), uuid.uuid4())

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.PENDING
    assert stored.webhook_attempts == 2
    assert stored.webhook_last_error == "HTTP 503"
    assert stored.status is PaymentStatus.SUCCEEDED


@pytest.mark.database
async def test_permanent_failure_marks_webhook_failed(
    session: AsyncSession, client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(410))

    with pytest.raises(WebhookPermanentError):
        await webhook_service(session, client).deliver(read(payment), uuid.uuid4())

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.FAILED
    assert stored.webhook_attempts == 1
    assert stored.webhook_last_error == "HTTP 410"
    assert stored.status is PaymentStatus.SUCCEEDED


@pytest.mark.database
async def test_success_after_failures_clears_last_error(
    session: AsyncSession, client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(HOOK_URL).mock(
        side_effect=[httpx.Response(500), httpx.Response(200)]
    )
    service = webhook_service(session, client)

    with pytest.raises(WebhookTransientError):
        await service.deliver(read(payment), uuid.uuid4())
    await service.deliver(read(payment), uuid.uuid4())

    stored = await reload(session, payment.id)
    assert stored.webhook_status is WebhookStatus.DELIVERED
    assert stored.webhook_attempts == 2
    assert stored.webhook_last_error is None


@pytest.mark.database
async def test_long_errors_are_truncated(
    session: AsyncSession, http_client: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    payment = await processed_payment(session)
    respx_mock.post(HOOK_URL).mock(side_effect=httpx.ConnectError("x" * 5000))
    settings = webhook_settings(WEBHOOK_LAST_ERROR_MAX_LENGTH=50)
    client = WebhookClient(http_client, settings, resolver=public_resolver)

    with pytest.raises(WebhookTransientError):
        await WebhookService(session, client, settings).deliver(
            read(payment), uuid.uuid4()
        )

    stored = await reload(session, payment.id)
    assert stored.webhook_last_error is not None
    assert len(stored.webhook_last_error) <= 50


@pytest.mark.database
async def test_mark_failed_does_not_override_delivered(
    session: AsyncSession, client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    delivered = await processed_payment(session)
    pending = await processed_payment(session)
    respx_mock.post(HOOK_URL).mock(return_value=httpx.Response(200))
    service = webhook_service(session, client)
    await service.deliver(read(delivered), uuid.uuid4())

    await service.mark_failed(delivered.id)
    await service.mark_failed(pending.id)

    assert (
        await reload(session, delivered.id)
    ).webhook_status is WebhookStatus.DELIVERED
    assert (await reload(session, pending.id)).webhook_status is WebhookStatus.FAILED


async def test_from_settings_configures_http_client() -> None:
    settings = webhook_settings(WEBHOOK_TIMEOUT=2.5, WEBHOOK_USER_AGENT="test-agent/1")

    async with WebhookClient.from_settings(settings) as webhook_client:
        http = webhook_client.http
        assert http.follow_redirects is False
        assert http.timeout.read == 2.5
        assert http.headers["User-Agent"] == "test-agent/1"
        assert webhook_client.secret == SECRET

    assert http.is_closed


def test_from_settings_refuses_missing_secret_before_opening_http() -> None:
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRET"):
        WebhookClient.from_settings(webhook_settings(WEBHOOK_SECRET=None))


async def test_http_client_is_closed_when_block_raises() -> None:
    with pytest.raises(RuntimeError, match="boom"):
        async with WebhookClient.from_settings(webhook_settings()) as webhook_client:
            http = webhook_client.http
            raise RuntimeError("boom")

    assert http.is_closed

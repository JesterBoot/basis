import ipaddress
import json
import socket

import httpx
import pytest
import respx

from services.exceptions import WebhookPermanentError, WebhookTransientError
from services.webhooks import WebhookClient
from tests.support.webhooks import (
    WEBHOOK_SECRET,
    WEBHOOK_URL,
    make_payload,
    webhook_settings,
)
from utils.network import IPAddress
from utils.signing import verify


@pytest.mark.parametrize("secret", [None, "", "   "])
def test_client_requires_a_secret(
    webhook_http: httpx.AsyncClient, secret: str | None
) -> None:
    with pytest.raises(RuntimeError, match="WEBHOOK_SECRET"):
        WebhookClient(webhook_http, webhook_settings(WEBHOOK_SECRET=secret))


async def test_request_is_signed_and_has_headers(
    webhook_client: WebhookClient, respx_mock: respx.MockRouter
) -> None:
    route = respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
    payload = make_payload()

    await webhook_client.send(WEBHOOK_URL, payload)

    request = route.calls.last.request
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["X-Event-Id"] == str(payload.event_id)
    assert request.headers["User-Agent"] == "basis-payments-webhook/1.0"
    assert verify(
        WEBHOOK_SECRET, request.content, request.headers["X-Webhook-Signature"]
    )
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
    webhook_client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(status_code))

    await webhook_client.send(WEBHOOK_URL, make_payload())


@pytest.mark.parametrize("status_code", [408, 429, 500, 502, 503, 504])
async def test_retryable_statuses_are_transient(
    webhook_client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(status_code))

    with pytest.raises(WebhookTransientError, match=f"HTTP {status_code}"):
        await webhook_client.send(WEBHOOK_URL, make_payload())


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 410, 422])
async def test_client_errors_are_permanent(
    webhook_client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(return_value=httpx.Response(status_code))

    with pytest.raises(WebhookPermanentError, match=f"HTTP {status_code}"):
        await webhook_client.send(WEBHOOK_URL, make_payload())


@pytest.mark.parametrize("status_code", [301, 302, 307, 308])
async def test_redirects_are_permanent_and_not_followed(
    webhook_client: WebhookClient, respx_mock: respx.MockRouter, status_code: int
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(
        return_value=httpx.Response(
            status_code, headers={"Location": "http://10.0.0.1/"}
        )
    )
    internal = respx_mock.post("http://10.0.0.1/")

    with pytest.raises(WebhookPermanentError, match=f"HTTP {status_code}"):
        await webhook_client.send(WEBHOOK_URL, make_payload())

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
    webhook_client: WebhookClient, respx_mock: respx.MockRouter, error: Exception
) -> None:
    respx_mock.post(WEBHOOK_URL).mock(side_effect=error)

    with pytest.raises(WebhookTransientError):
        await webhook_client.send(WEBHOOK_URL, make_payload())


@pytest.mark.parametrize(
    "url",
    ["http://127.0.0.1/hook", "http://localhost:9000/hook", "http://[::1]/hook"],
)
async def test_private_literal_hosts_are_blocked_without_request(
    webhook_client: WebhookClient, respx_mock: respx.MockRouter, url: str
) -> None:
    route = respx_mock.post(url)

    with pytest.raises(WebhookPermanentError, match="private"):
        await webhook_client.send(url, make_payload())

    assert not route.called


async def test_public_name_resolving_to_private_ip_is_blocked(
    webhook_http: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    async def rebinding_resolver(host: str, port: int) -> list[IPAddress]:
        return [ipaddress.ip_address("8.8.8.8"), ipaddress.ip_address("10.0.0.7")]

    client = WebhookClient(
        webhook_http, webhook_settings(), resolver=rebinding_resolver
    )
    route = respx_mock.post(WEBHOOK_URL)

    with pytest.raises(
        WebhookPermanentError, match="resolves to private address 10.0.0.7"
    ):
        await client.send(WEBHOOK_URL, make_payload())

    assert not route.called


async def test_dns_failure_is_transient(webhook_http: httpx.AsyncClient) -> None:
    async def failing_resolver(host: str, port: int) -> list[IPAddress]:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    client = WebhookClient(webhook_http, webhook_settings(), resolver=failing_resolver)

    with pytest.raises(WebhookTransientError, match="DNS lookup failed"):
        await client.send(WEBHOOK_URL, make_payload())


async def test_private_destinations_allowed_when_configured(
    webhook_http: httpx.AsyncClient, respx_mock: respx.MockRouter
) -> None:
    settings = webhook_settings(WEBHOOK_ALLOW_PRIVATE=True)

    async def unused_resolver(host: str, port: int) -> list[IPAddress]:
        raise AssertionError("resolver must not be called")

    client = WebhookClient(webhook_http, settings, resolver=unused_resolver)
    route = respx_mock.post("http://webhook-sink:9000/hook").mock(
        return_value=httpx.Response(204)
    )

    await client.send("http://webhook-sink:9000/hook", make_payload())

    assert route.called


async def test_resolver_receives_default_port(webhook_http: httpx.AsyncClient) -> None:
    seen: list[tuple[str, int]] = []

    async def recording_resolver(host: str, port: int) -> list[IPAddress]:
        seen.append((host, port))
        return [ipaddress.ip_address("93.184.216.34")]

    client = WebhookClient(
        webhook_http, webhook_settings(), resolver=recording_resolver
    )
    with respx.mock:
        respx.post(WEBHOOK_URL).mock(return_value=httpx.Response(200))
        respx.post("http://merchant.example:8443/hook").mock(
            return_value=httpx.Response(200)
        )
        await client.send(WEBHOOK_URL, make_payload())
        await client.send("http://merchant.example:8443/hook", make_payload())

    assert seen == [("merchant.example", 443), ("merchant.example", 8443)]


async def test_from_settings_configures_http_client() -> None:
    settings = webhook_settings(WEBHOOK_TIMEOUT=2.5, WEBHOOK_USER_AGENT="test-agent/1")

    async with WebhookClient.from_settings(settings) as webhook_client:
        http = webhook_client.http
        assert http.follow_redirects is False
        assert http.timeout.read == 2.5
        assert http.headers["User-Agent"] == "test-agent/1"
        assert webhook_client.secret == WEBHOOK_SECRET

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

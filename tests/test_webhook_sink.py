import json
import uuid
from collections.abc import AsyncIterator

import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from tools.webhook_sink import create_sink_app
from utils.signing import sign

SECRET = "sink-secret"


@pytest_asyncio.fixture
async def sink() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=create_sink_app(secret=SECRET))
    async with AsyncClient(transport=transport, base_url="http://sink") as client:
        yield client


def webhook(
    payment_id: str, event_id: str | None = None
) -> tuple[bytes, dict[str, str]]:
    event = event_id or str(uuid.uuid4())
    body = json.dumps(
        {"event_id": event, "payment_id": payment_id, "status": "succeeded"}
    ).encode()
    headers = {
        "Content-Type": "application/json",
        "X-Event-Id": event,
        "X-Webhook-Signature": sign(SECRET, body),
    }
    return body, headers


async def test_health(sink: AsyncClient) -> None:
    assert (await sink.get("/health")).json() == {"status": "ok"}


async def test_records_signed_call(sink: AsyncClient) -> None:
    body, headers = webhook("pay-1")

    response = await sink.post("/hook", content=body, headers=headers)
    calls = (await sink.get("/calls")).json()

    assert response.status_code == 200
    assert len(calls) == 1
    call = calls[0]
    assert call["payment_id"] == "pay-1"
    assert call["status"] == "succeeded"
    assert call["event_id"] == headers["X-Event-Id"]
    assert call["signature_valid"] is True
    assert call["duplicate"] is False
    assert call["responded_with"] == 200


async def test_invalid_signature_is_recorded_as_invalid(sink: AsyncClient) -> None:
    body, headers = webhook("pay-1")
    headers["X-Webhook-Signature"] = "sha256=forged"

    await sink.post("/hook", content=body, headers=headers)

    assert (await sink.get("/calls")).json()[0]["signature_valid"] is False


async def test_same_event_twice_is_marked_duplicate(sink: AsyncClient) -> None:
    body, headers = webhook("pay-1")

    await sink.post("/hook", content=body, headers=headers)
    await sink.post("/hook", content=body, headers=headers)

    assert [call["duplicate"] for call in (await sink.get("/calls")).json()] == [
        False,
        True,
    ]


async def test_fail_first_calls_then_succeed(sink: AsyncClient) -> None:
    await sink.post("/config", json={"fail_times": 2, "fail_status_code": 503})
    body, headers = webhook("pay-1")

    codes = [
        (await sink.post("/hook", content=body, headers=headers)).status_code
        for _ in range(3)
    ]

    assert codes == [503, 503, 200]


async def test_fixed_failure_status(sink: AsyncClient) -> None:
    await sink.post("/config", json={"status_code": 410})
    body, headers = webhook("pay-1")

    response = await sink.post("/hook", content=body, headers=headers)

    assert response.status_code == 410


async def test_filter_and_reset_calls(sink: AsyncClient) -> None:
    for payment_id in ("pay-1", "pay-2", "pay-1"):
        body, headers = webhook(payment_id)
        await sink.post("/hook", content=body, headers=headers)

    assert len((await sink.get("/calls", params={"payment_id": "pay-1"})).json()) == 2
    assert (await sink.delete("/calls")).status_code == 204
    assert (await sink.get("/calls")).json() == []


async def test_reset_rearms_fail_times(sink: AsyncClient) -> None:
    await sink.post("/config", json={"fail_times": 1})
    body, headers = webhook("pay-1")
    await sink.post("/hook", content=body, headers=headers)
    await sink.delete("/calls")

    response = await sink.post("/hook", content=body, headers=headers)

    assert response.status_code == 503


async def test_non_json_body_is_still_recorded(sink: AsyncClient) -> None:
    response = await sink.post("/hook", content=b"plain text")

    call = (await sink.get("/calls")).json()[0]
    assert response.status_code == 200
    assert call["body"] is None
    assert call["signature_valid"] is False


async def test_rejects_invalid_config(sink: AsyncClient) -> None:
    response = await sink.post("/config", json={"status_code": 999})

    assert response.status_code == 422
    assert (await sink.get("/config")).json()["status_code"] == 200

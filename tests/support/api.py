import uuid
from typing import Any

from httpx import AsyncClient, Response

TEST_API_KEY = "test-api-key"
PAYMENTS_URL = "/api/v1/payments"

PAYMENT_BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/hooks/payments",
}


def new_idempotency_key() -> str:
    return f"test-{uuid.uuid4()}"


async def post_payment(
    client: AsyncClient, body: dict[str, Any], idempotency_key: str
) -> Response:
    return await client.post(
        PAYMENTS_URL, json=body, headers={"Idempotency-Key": idempotency_key}
    )


def nested_metadata(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {"leaf": 1}
    for _ in range(depth):
        value = {"a": value}
    return value

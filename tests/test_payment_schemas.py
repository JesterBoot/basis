from decimal import Decimal
from typing import Any

import pytest
from pydantic import ValidationError

from core.config import Settings, get_settings
from schemas.payment import PaymentCreate

VALID_BODY: dict[str, Any] = {
    "amount": "100.00",
    "currency": "USD",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/hooks/payments",
}


def body(**overrides: Any) -> dict[str, Any]:
    return {**VALID_BODY, **overrides}


@pytest.fixture
def private_webhooks_forbidden(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(_env_file=None, WEBHOOK_ALLOW_PRIVATE=False)
    monkeypatch.setattr("schemas.payment.get_settings", lambda: settings)


@pytest.fixture
def private_webhooks_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(_env_file=None, WEBHOOK_ALLOW_PRIVATE=True)
    monkeypatch.setattr("schemas.payment.get_settings", lambda: settings)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("100.00", Decimal("100.00")),
        (100, Decimal(100)),
        (0.01, Decimal("0.01")),
        ("9999999999999999.99", Decimal("9999999999999999.99")),
    ],
)
def test_valid_amounts_are_accepted(value: Any, expected: Decimal) -> None:
    assert PaymentCreate.model_validate(body(amount=value)).amount == expected


@pytest.mark.parametrize(
    "value",
    [
        "0",
        0,
        "-1.00",
        "abc",
        "10.999",
        "1e30",
        "99999999999999999.99",
        "NaN",
        "Infinity",
        True,
        None,
    ],
    ids=[
        "zero-str",
        "zero",
        "negative",
        "text",
        "three-dp",
        "huge-exp",
        "too-many-digits",
        "nan",
        "inf",
        "bool",
        "null",
    ],
)
def test_invalid_amounts_are_rejected(value: Any) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(amount=value))


@pytest.mark.parametrize("value", ["usd", "GBP", "", "USDT"])
def test_unknown_or_lowercase_currency_is_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(currency=value))


@pytest.mark.parametrize("value", ["", "   ", "x" * 1001])
def test_description_length_is_enforced(value: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(description=value))


def test_description_is_stripped() -> None:
    assert (
        PaymentCreate.model_validate(body(description="  Order  ")).description
        == "Order"
    )


def test_metadata_defaults_to_empty_object() -> None:
    payload = {key: value for key, value in VALID_BODY.items() if key != "metadata"}

    assert PaymentCreate.model_validate(payload).metadata == {}


@pytest.mark.parametrize(
    "value", [[1, 2], "text", 5], ids=["array", "string", "number"]
)
def test_metadata_must_be_an_object(value: Any) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(metadata=value))


def test_metadata_size_limit() -> None:
    max_bytes = get_settings().payment_metadata_max_bytes
    at_limit = {"k": "x" * (max_bytes - len('{"k": ""}'))}
    over_limit = {"k": "x" * max_bytes}

    assert PaymentCreate.model_validate(body(metadata=at_limit)).metadata == at_limit
    with pytest.raises(ValidationError, match="metadata must not exceed"):
        PaymentCreate.model_validate(body(metadata=over_limit))


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(status="succeeded"))


@pytest.mark.parametrize(
    "value",
    [
        "ftp://merchant.example/hook",
        "not a url",
        "merchant.example/hook",
        "https://",
        "https://e.com/" + "a" * 2048,
    ],
    ids=["ftp", "garbage", "no-scheme", "no-host", "too-long"],
)
def test_invalid_webhook_urls_are_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(webhook_url=value))


@pytest.mark.parametrize(
    "value",
    [
        "http://localhost:9000/hook",
        "http://api.localhost/hook",
        "http://127.0.0.1/hook",
        "http://10.0.0.5/hook",
        "http://192.168.1.10/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/hook",
    ],
)
@pytest.mark.usefixtures("private_webhooks_forbidden")
def test_private_webhook_hosts_are_rejected_by_default(value: str) -> None:
    with pytest.raises(ValidationError, match="public host"):
        PaymentCreate.model_validate(body(webhook_url=value))


@pytest.mark.usefixtures("private_webhooks_allowed")
def test_private_webhook_hosts_allowed_when_configured() -> None:
    payload = PaymentCreate.model_validate(
        body(webhook_url="http://webhook-sink:9000/hook")
    )

    assert payload.webhook_url.host == "webhook-sink"
    assert PaymentCreate.model_validate(body(webhook_url="http://127.0.0.1/hook"))


@pytest.mark.usefixtures("private_webhooks_forbidden")
def test_docker_service_names_are_not_treated_as_private() -> None:
    assert PaymentCreate.model_validate(
        body(webhook_url="http://webhook-sink:9000/hook")
    )


def hash_of(payload: dict[str, Any]) -> str:
    return PaymentCreate.model_validate(payload).request_hash()


@pytest.mark.parametrize(
    "variant",
    [
        body(amount=100),
        body(amount="100"),
        body(amount="100.0"),
        body(amount=100.00),
        {key: VALID_BODY[key] for key in reversed(list(VALID_BODY))},
        body(description="  Order #42  "),
    ],
    ids=["int", "str-no-dp", "one-dp", "float", "reordered-keys", "padded-description"],
)
def test_equivalent_requests_have_the_same_hash(variant: dict[str, Any]) -> None:
    assert hash_of(variant) == hash_of(VALID_BODY)


def test_metadata_key_order_does_not_change_hash() -> None:
    first = body(metadata={"a": 1, "b": {"x": 1, "y": 2}})
    second = body(metadata={"b": {"y": 2, "x": 1}, "a": 1})

    assert hash_of(first) == hash_of(second)


@pytest.mark.parametrize(
    "variant",
    [
        body(amount="100.01"),
        body(currency="EUR"),
        body(description="Order #43"),
        body(metadata={"order_id": 43}),
        body(metadata={}),
        body(webhook_url="https://merchant.example/hooks/other"),
    ],
    ids=["amount", "currency", "description", "metadata", "empty-metadata", "webhook"],
)
def test_different_requests_have_different_hashes(variant: dict[str, Any]) -> None:
    assert hash_of(variant) != hash_of(VALID_BODY)


def test_hash_is_sha256_hex() -> None:
    digest = hash_of(VALID_BODY)

    assert len(digest) == 64
    assert int(digest, 16) >= 0

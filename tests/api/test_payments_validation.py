from typing import Any

import pytest
from httpx import AsyncClient, Response

from db.session import AsyncSession
from models import Payment
from tests.support.api import (
    PAYMENT_BODY,
    PAYMENTS_URL,
    nested_metadata,
    new_idempotency_key,
    post_payment,
)
from tests.support.database import count_rows

METADATA_LIMIT_BYTES = 16 * 1024

RAW_BODY = (
    '{"amount":"1.00","currency":"USD","description":%s,'
    '"metadata":%s,"webhook_url":"https://merchant.example/hooks"}'
)


async def post_raw(client: AsyncClient, content: bytes) -> Response:
    return await client.post(
        PAYMENTS_URL,
        content=content,
        headers={
            "Idempotency-Key": new_idempotency_key(),
            "Content-Type": "application/json",
        },
    )


@pytest.mark.parametrize(
    "idempotency_key",
    ["", "a" * 256, "has space", "slash/key", "кириллица"],
    ids=["empty", "too-long", "space", "slash", "non-ascii"],
)
async def test_invalid_idempotency_key_is_rejected(
    client: AsyncClient, idempotency_key: str
) -> None:
    response = await client.post(
        PAYMENTS_URL,
        json=PAYMENT_BODY,
        headers={"Idempotency-Key": idempotency_key.encode()},
    )

    assert response.status_code == 422


async def test_missing_idempotency_key_is_rejected(client: AsyncClient) -> None:
    response = await client.post(PAYMENTS_URL, json=PAYMENT_BODY)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


@pytest.mark.parametrize(
    "idempotency_key",
    ["a+b", "a@b", "a=b", "a%20b", "a,b", "a*b", "a\tb", "a;b"],
    ids=["plus", "at", "equals", "percent", "comma", "star", "tab", "semicolon"],
)
async def test_e_a3_illegal_key_charset_is_rejected(
    client: AsyncClient, idempotency_key: str
) -> None:
    response = await post_payment(client, PAYMENT_BODY, idempotency_key)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


@pytest.mark.database
@pytest.mark.parametrize(
    "idempotency_key",
    ["k" * 255, "order_42-retry:1.0", "x", "UPPER.lower_09"],
    ids=["max-length", "all-punctuation", "single-char", "mixed-case"],
)
async def test_e_a3_boundary_keys_are_accepted_and_stored_verbatim(
    client: AsyncClient, session: AsyncSession, idempotency_key: str
) -> None:
    created = await post_payment(client, PAYMENT_BODY, idempotency_key)

    assert created.status_code == 202
    fetched = await client.get(f"{PAYMENTS_URL}/{created.json()['payment_id']}")
    assert fetched.json()["idempotency_key"] == idempotency_key


async def test_invalid_body_is_rejected(client: AsyncClient) -> None:
    response = await post_payment(
        client, {**PAYMENT_BODY, "amount": "10.999"}, new_idempotency_key()
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "amount"]


@pytest.mark.parametrize(
    "content",
    [b"[]", b'"payment"', b"null", b"42", b"", b"{", b'{"amount": }'],
    ids=["array", "string", "null", "number", "empty", "truncated", "malformed"],
)
async def test_e_a9_non_object_or_malformed_body_is_rejected(
    client: AsyncClient, content: bytes
) -> None:
    response = await post_raw(client, content)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][0] == "body"


@pytest.mark.parametrize(
    "amount",
    [1e30, 0.30000000000000004, -0.01, 0.001, True, [], {"value": 1}],
    ids=["huge-float", "float-noise", "negative", "three-dp", "bool", "list", "obj"],
)
async def test_e_a9_json_number_amounts_out_of_bounds_are_rejected(
    client: AsyncClient, amount: Any
) -> None:
    response = await post_payment(
        client, {**PAYMENT_BODY, "amount": amount}, new_idempotency_key()
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "amount"]


async def test_e_a11_metadata_limit_counts_utf8_bytes_not_characters(
    client: AsyncClient,
) -> None:
    multibyte = {"k": "ж" * (METADATA_LIMIT_BYTES // 2)}

    response = await post_payment(
        client, {**PAYMENT_BODY, "metadata": multibyte}, new_idempotency_key()
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "metadata"]


@pytest.mark.parametrize(
    ("description", "metadata"),
    [
        ('"a\\u0000b"', "{}"),
        ('"d"', '{"k":"a\\u0000b"}'),
        ('"d"', '{"a\\u0000":1}'),
        ('"d"', '{"k":NaN}'),
        ('"d"', '{"k":Infinity}'),
        ('"\\ud800"', "{}"),
        ('"d"', '{"k":"\\ud800"}'),
    ],
    ids=[
        "nul-in-description",
        "nul-in-metadata-value",
        "nul-in-metadata-key",
        "nan-in-metadata",
        "infinity-in-metadata",
        "lone-surrogate-description",
        "lone-surrogate-metadata",
    ],
)
@pytest.mark.database
async def test_e_a11_unstorable_text_is_rejected_with_422(
    client: AsyncClient, session: AsyncSession, description: str, metadata: str
) -> None:
    response = await post_raw(client, (RAW_BODY % (description, metadata)).encode())

    assert response.status_code == 422
    assert await count_rows(session, Payment) == 0


@pytest.mark.database
async def test_e_a11_pathologically_deep_metadata_is_rejected_with_422(
    client: AsyncClient, session: AsyncSession
) -> None:
    depth = 2500
    metadata = '{"a":' * depth + "1" + "}" * depth

    response = await post_raw(client, (RAW_BODY % ('"d"', metadata)).encode())

    assert response.status_code == 422
    assert await count_rows(session, Payment) == 0


@pytest.mark.database
async def test_e_a11_metadata_depth_limit_boundary(
    client: AsyncClient, session: AsyncSession
) -> None:
    at_limit = await post_payment(
        client,
        {**PAYMENT_BODY, "metadata": {"tree": nested_metadata(30)}},
        new_idempotency_key(),
    )
    over_limit = await post_payment(
        client,
        {**PAYMENT_BODY, "metadata": {"tree": nested_metadata(31)}},
        new_idempotency_key(),
    )

    assert at_limit.status_code == 202
    assert over_limit.status_code == 422
    assert "nested deeper than 32" in over_limit.json()["detail"][0]["msg"]


async def test_validation_errors_do_not_echo_input(client: AsyncClient) -> None:
    response = await post_payment(
        client,
        {**PAYMENT_BODY, "amount": "secret-looking-input"},
        new_idempotency_key(),
    )

    assert response.status_code == 422
    assert all("input" not in error for error in response.json()["detail"])


async def test_e_a12_nat64_and_multicast_hosts_are_rejected(
    client: AsyncClient,
) -> None:
    for webhook_url in ("http://[64:ff9b::7f00:1]/hook", "http://224.0.0.1/hook"):
        response = await post_payment(
            client, {**PAYMENT_BODY, "webhook_url": webhook_url}, new_idempotency_key()
        )

        assert response.status_code == 422, webhook_url


@pytest.mark.parametrize(
    "webhook_url",
    [
        "http://[::ffff:127.0.0.1]/hook",
        "http://[::ffff:169.254.169.254]/latest",
        "http://[0:0:0:0:0:ffff:7f00:1]/hook",
        "http://0.0.0.0/hook",
        "http://0/hook",
        "http://LOCALHOST/hook",
        "http://localhost./hook",
        "http://Api.LocalHost./hook",
        "http://2130706433/hook",
        "http://0x7f000001/hook",
        "http://017700000001/hook",
        "http://127.1/hook",
        "http://ｌｏｃａｌｈｏｓｔ/hook",
        "http://１２７.０.０.１/hook",
        "http://merchant.example@127.0.0.1/hook",
        "http://127.0.0.1#@merchant.example/hook",
        "http://[::]/hook",
        "http://[fe80::1]/hook",
        "http://[fc00::1]/hook",
        "http://[2002:7f00:1::]/hook",
        "http://100.64.0.1/hook",
        "http://172.16.0.1/hook",
        "http://255.255.255.255/hook",
    ],
    ids=[
        "ipv4-mapped-loopback",
        "ipv4-mapped-metadata",
        "ipv4-mapped-hex",
        "unspecified-v4",
        "zero-shorthand",
        "uppercase-localhost",
        "localhost-trailing-dot",
        "sub-localhost-mixed-case",
        "decimal-int-ip",
        "hex-ip",
        "octal-ip",
        "short-ip",
        "fullwidth-localhost",
        "fullwidth-digits",
        "userinfo-trick",
        "fragment-trick",
        "unspecified-v6",
        "link-local-v6",
        "ula-v6",
        "6to4-loopback",
        "cgnat",
        "rfc1918-172",
        "broadcast",
    ],
)
async def test_e_a12_obfuscated_private_hosts_are_rejected(
    client: AsyncClient, webhook_url: str
) -> None:
    response = await post_payment(
        client, {**PAYMENT_BODY, "webhook_url": webhook_url}, new_idempotency_key()
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "webhook_url"]


@pytest.mark.database
async def test_e_a12_localhost_in_userinfo_does_not_block_public_host(
    client: AsyncClient, session: AsyncSession
) -> None:
    webhook_url = "http://localhost:80@merchant.example/hook"

    response = await post_payment(
        client, {**PAYMENT_BODY, "webhook_url": webhook_url}, new_idempotency_key()
    )

    assert response.status_code == 202

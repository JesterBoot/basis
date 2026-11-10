import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient, Response
from sqlalchemy import func, insert, select, update
from sqlalchemy.exc import (
    DataError,
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from core.config import Settings
from db.session import AsyncSession, get_session
from main import create_app
from models import OutboxEvent, Payment, PaymentStatus
from tests.support.api import TEST_API_KEY

PAYMENTS = "/api/v1/payments"

BODY: dict[str, Any] = {
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/hooks/payments",
}

METADATA_LIMIT_BYTES = 16 * 1024

RAW_BODY = (
    '{"amount":"1.00","currency":"USD","description":%s,'
    '"metadata":%s,"webhook_url":"https://merchant.example/hooks"}'
)


def key() -> str:
    return f"qa-{uuid.uuid4()}"


def nested(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {"leaf": 1}
    for _ in range(depth):
        value = {"a": value}
    return value


async def post(
    api: AsyncClient, body: dict[str, Any], idempotency_key: str
) -> Response:
    return await api.post(
        PAYMENTS, json=body, headers={"Idempotency-Key": idempotency_key}
    )


async def post_raw(api: AsyncClient, content: bytes) -> Response:
    return await api.post(
        PAYMENTS,
        content=content,
        headers={"Idempotency-Key": key(), "Content-Type": "application/json"},
    )


async def count(session: AsyncSession, model: type[Payment] | type[OutboxEvent]) -> int:
    return await session.scalar(select(func.count()).select_from(model)) or 0


@pytest.fixture
def application() -> FastAPI:
    return create_app()


@pytest.fixture
async def api(application: FastAPI) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=application, raise_app_exceptions=False),
        base_url="http://testserver",
        headers={"X-API-Key": TEST_API_KEY},
    ) as http_client:
        yield http_client


@pytest.fixture
def session_entries(application: FastAPI) -> list[str]:
    entries: list[str] = []

    async def recording_session() -> AsyncIterator[AsyncSession]:
        entries.append("entered")
        raise HTTPException(status_code=418)
        yield

    application.dependency_overrides[get_session] = recording_session
    return entries


# E-A1: authentication


@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", PAYMENTS), ("GET", f"{PAYMENTS}/{uuid.uuid4()}")],
    ids=["create", "get"],
)
async def test_e_a1_wrong_key_never_opens_db_session(
    api: AsyncClient, session_entries: list[str], method: str, path: str
) -> None:
    response = await api.request(
        method,
        path,
        json=BODY,
        headers={"X-API-Key": "wrong-key", "Idempotency-Key": key()},
    )

    assert response.status_code == 401
    assert session_entries == []


async def test_e_a1_valid_key_reaches_db_session(
    api: AsyncClient, session_entries: list[str]
) -> None:
    response = await api.get(f"{PAYMENTS}/{uuid.uuid4()}")

    assert response.status_code == 418
    assert session_entries == ["entered"]


async def test_e_a1_header_name_is_case_insensitive(api: AsyncClient) -> None:
    api.headers.pop("X-API-Key")

    response = await api.post(PAYMENTS, json={}, headers={"x-api-key": TEST_API_KEY})

    assert response.status_code == 422


@pytest.mark.parametrize(
    "provided",
    [
        f" {TEST_API_KEY} ",
        TEST_API_KEY.upper(),
        f"{TEST_API_KEY}x",
        TEST_API_KEY[:-1],
        f"Bearer {TEST_API_KEY}",
    ],
    ids=["padded", "uppercase", "suffix", "prefix", "bearer"],
)
async def test_e_a1_near_miss_keys_are_rejected(
    api: AsyncClient, provided: str
) -> None:
    response = await api.post(PAYMENTS, json={}, headers={"X-API-Key": provided})

    assert response.status_code == 401


async def test_e_a1_unauthorized_response_advertises_scheme(api: AsyncClient) -> None:
    api.headers.pop("X-API-Key")

    response = await api.post(PAYMENTS, json={})

    assert response.headers["WWW-Authenticate"] == "APIKey"
    assert response.headers["Content-Type"] == "application/json"


@pytest.mark.parametrize("blank", ["", "   "], ids=["empty", "whitespace"])
def test_e_a1_api_refuses_to_start_with_blank_api_key(
    monkeypatch: pytest.MonkeyPatch, blank: str
) -> None:
    settings = Settings(_env_file=None, API_KEY=blank)
    monkeypatch.setattr("core.setup.get_settings", lambda: settings)

    with (
        pytest.raises(RuntimeError, match="API_KEY must be set"),
        TestClient(create_app()),
    ):
        pass


# E-A2 / E-A3: Idempotency-Key header


@pytest.mark.parametrize(
    "idempotency_key",
    ["a+b", "a@b", "a=b", "a%20b", "a,b", "a*b", "a\tb", "a;b"],
    ids=["plus", "at", "equals", "percent", "comma", "star", "tab", "semicolon"],
)
async def test_e_a3_illegal_key_charset_is_rejected(
    api: AsyncClient, idempotency_key: str
) -> None:
    response = await post(api, BODY, idempotency_key)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "Idempotency-Key"]


@pytest.mark.database
@pytest.mark.parametrize(
    "idempotency_key",
    ["k" * 255, "order_42-retry:1.0", "x", "UPPER.lower_09"],
    ids=["max-length", "all-punctuation", "single-char", "mixed-case"],
)
async def test_e_a3_boundary_keys_are_accepted_and_stored_verbatim(
    api: AsyncClient, session: AsyncSession, idempotency_key: str
) -> None:
    created = await post(api, BODY, idempotency_key)

    assert created.status_code == 202
    fetched = await api.get(f"{PAYMENTS}/{created.json()['payment_id']}")
    assert fetched.json()["idempotency_key"] == idempotency_key


@pytest.mark.database
async def test_e_a4_idempotency_key_is_case_sensitive(
    api: AsyncClient, session: AsyncSession
) -> None:
    first = await post(api, BODY, "Order-Key")

    second = await post(api, BODY, "order-key")

    assert second.status_code == 202
    assert "Idempotent-Replayed" not in second.headers
    assert second.json()["payment_id"] != first.json()["payment_id"]
    assert await count(session, Payment) == 2


# Request body shape and validation (E-A9, E-A11)


@pytest.mark.parametrize(
    "content",
    [b"[]", b'"payment"', b"null", b"42", b"", b"{", b'{"amount": }'],
    ids=["array", "string", "null", "number", "empty", "truncated", "malformed"],
)
async def test_e_a9_non_object_or_malformed_body_is_rejected(
    api: AsyncClient, content: bytes
) -> None:
    response = await post_raw(api, content)

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"][0] == "body"


@pytest.mark.parametrize(
    "amount",
    [1e30, 0.30000000000000004, -0.01, 0.001, True, [], {"value": 1}],
    ids=["huge-float", "float-noise", "negative", "three-dp", "bool", "list", "obj"],
)
async def test_e_a9_json_number_amounts_out_of_bounds_are_rejected(
    api: AsyncClient, amount: Any
) -> None:
    response = await post(api, {**BODY, "amount": amount}, key())

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "amount"]


async def test_e_a11_metadata_limit_counts_utf8_bytes_not_characters(
    api: AsyncClient,
) -> None:
    multibyte = {"k": "ж" * (METADATA_LIMIT_BYTES // 2)}

    response = await post(api, {**BODY, "metadata": multibyte}, key())

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
    api: AsyncClient, session: AsyncSession, description: str, metadata: str
) -> None:
    response = await post_raw(api, (RAW_BODY % (description, metadata)).encode())

    assert response.status_code == 422
    assert await count(session, Payment) == 0


@pytest.mark.database
async def test_e_a11_pathologically_deep_metadata_is_rejected_with_422(
    api: AsyncClient, session: AsyncSession
) -> None:
    depth = 2500
    metadata = '{"a":' * depth + "1" + "}" * depth

    response = await post_raw(api, (RAW_BODY % ('"d"', metadata)).encode())

    assert response.status_code == 422
    assert await count(session, Payment) == 0


@pytest.mark.database
async def test_e_a7_deep_unicode_metadata_round_trips_and_replays(
    api: AsyncClient, session: AsyncSession
) -> None:
    metadata = {"ключ": "значение 🍕", "tree": nested(20), "big": 10**40}
    body = {**BODY, "description": "Заказ №42 🍕", "metadata": metadata}
    idempotency_key = key()
    created = await post(api, body, idempotency_key)

    replay = await post(api, body, idempotency_key)
    fetched = await api.get(f"{PAYMENTS}/{created.json()['payment_id']}")

    assert created.status_code == 202
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert fetched.json()["metadata"] == metadata
    assert fetched.json()["description"] == "Заказ №42 🍕"


@pytest.mark.database
async def test_e_a11_metadata_depth_limit_boundary(
    api: AsyncClient, session: AsyncSession
) -> None:
    at_limit = await post(api, {**BODY, "metadata": {"tree": nested(30)}}, key())
    over_limit = await post(api, {**BODY, "metadata": {"tree": nested(31)}}, key())

    assert at_limit.status_code == 202
    assert over_limit.status_code == 422
    assert "nested deeper than 32" in over_limit.json()["detail"][0]["msg"]


async def test_e_a12_nat64_and_multicast_hosts_are_rejected(api: AsyncClient) -> None:
    for webhook_url in ("http://[64:ff9b::7f00:1]/hook", "http://224.0.0.1/hook"):
        response = await post(api, {**BODY, "webhook_url": webhook_url}, key())

        assert response.status_code == 422, webhook_url


async def test_validation_errors_do_not_echo_input(api: AsyncClient) -> None:
    response = await post(api, {**BODY, "amount": "secret-looking-input"}, key())

    assert response.status_code == 422
    assert all("input" not in error for error in response.json()["detail"])


# E-A12: SSRF guard


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
    api: AsyncClient, webhook_url: str
) -> None:
    response = await post(api, {**BODY, "webhook_url": webhook_url}, key())

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "webhook_url"]


@pytest.mark.database
async def test_e_a12_localhost_in_userinfo_does_not_block_public_host(
    api: AsyncClient, session: AsyncSession
) -> None:
    webhook_url = "http://localhost:80@merchant.example/hook"

    response = await post(api, {**BODY, "webhook_url": webhook_url}, key())

    assert response.status_code == 202


# E-A4 / E-A6 / E-A7 / E-A8: idempotency through the HTTP API


@pytest.mark.database
async def test_e_a7_reordered_padded_and_case_variant_body_replays(
    api: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    first = await post(api, BODY, idempotency_key)
    variant = {
        "webhook_url": "https://MERCHANT.example/hooks/payments",
        "metadata": {"order_id": 42},
        "description": "  Order #42  ",
        "currency": "RUB",
        "amount": 1500.0,
    }

    second = await post(api, variant, idempotency_key)

    assert second.status_code == 202
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()


@pytest.mark.database
async def test_e_a6_conflict_writes_nothing_and_keeps_original(
    api: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    await post(api, BODY, idempotency_key)

    conflict = await post(api, {**BODY, "amount": "1500.01"}, idempotency_key)

    assert conflict.status_code == 409
    assert "Idempotent-Replayed" not in conflict.headers
    assert await count(session, Payment) == 1
    assert await count(session, OutboxEvent) == 1
    stored = await session.scalar(select(Payment))
    assert stored is not None
    assert str(stored.amount) == "1500.00"


@pytest.mark.database
async def test_e_a6_metadata_value_type_change_is_a_conflict(
    api: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    await post(api, BODY, idempotency_key)

    conflict = await post(
        api, {**BODY, "metadata": {"order_id": "42"}}, idempotency_key
    )

    assert conflict.status_code == 409


@pytest.mark.database
async def test_e_a8_replay_after_success_returns_current_status(
    api: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    created = (await post(api, BODY, idempotency_key)).json()
    await session.execute(
        update(Payment)
        .where(Payment.idempotency_key == idempotency_key)
        .values(status=PaymentStatus.SUCCEEDED)
    )
    await session.commit()

    replay = await post(api, BODY, idempotency_key)

    assert replay.status_code == 202
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json() == {**created, "status": "succeeded"}
    assert await count(session, OutboxEvent) == 1


@pytest.mark.database
async def test_e_a5_concurrent_mixed_bodies_yield_only_202_or_409(
    api: AsyncClient, session: AsyncSession
) -> None:
    idempotency_key = key()
    bodies = [BODY, {**BODY, "currency": "USD"}] * 10

    responses = await asyncio.gather(*(post(api, b, idempotency_key) for b in bodies))

    assert {r.status_code for r in responses} == {202, 409}
    accepted = [r for r in responses if r.status_code == 202]
    assert len({r.json()["payment_id"] for r in accepted}) == 1
    assert await count(session, Payment) == 1
    assert await count(session, OutboxEvent) == 1


@pytest.mark.database
async def test_e_a5_concurrent_distinct_keys_create_distinct_payments(
    api: AsyncClient, session: AsyncSession
) -> None:
    responses = await asyncio.gather(*(post(api, BODY, key()) for _ in range(20)))

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 20
    assert await count(session, OutboxEvent) == 20


@pytest.mark.database
async def test_e_a5_waiter_inserts_when_first_transaction_rolls_back(
    api: AsyncClient,
    session: AsyncSession,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    idempotency_key = key()

    async with session_factory() as holder:
        await holder.execute(
            insert(Payment).values(
                id=uuid.uuid4(),
                amount=BODY["amount"],
                currency=BODY["currency"],
                description="uncommitted competitor",
                meta={},
                idempotency_key=idempotency_key,
                request_hash="0" * 64,
                webhook_url=BODY["webhook_url"],
            )
        )
        task = asyncio.create_task(post(api, BODY, idempotency_key))
        await asyncio.sleep(0.2)
        blocked = not task.done()
        await holder.rollback()

    response = await task
    assert blocked
    assert response.status_code == 202
    assert "Idempotent-Replayed" not in response.headers
    assert await count(session, Payment) == 1
    assert await count(session, OutboxEvent) == 1


# E-A13: GET by id


@pytest.mark.database
@pytest.mark.parametrize(
    "transform",
    [str.upper, lambda value: value.replace("-", "")],
    ids=["uppercase", "no-hyphens"],
)
async def test_e_a13_alternate_uuid_spellings_resolve(
    api: AsyncClient, session: AsyncSession, transform: Any
) -> None:
    payment_id = (await post(api, BODY, key())).json()["payment_id"]

    response = await api.get(f"{PAYMENTS}/{transform(payment_id)}")

    assert response.status_code == 200
    assert response.json()["payment_id"] == payment_id


@pytest.mark.parametrize(
    "payment_id",
    [
        str(uuid.uuid4())[:-1],
        f"{uuid.uuid4()}0",
        "g" * 32,
        "123",
        "00000000-0000-0000-0000-00000000000",
    ],
    ids=["truncated", "extra-char", "non-hex", "integer", "short-group"],
)
async def test_e_a13_near_miss_uuids_are_rejected(
    api: AsyncClient, payment_id: str
) -> None:
    response = await api.get(f"{PAYMENTS}/{payment_id}")

    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["path", "payment_id"]


@pytest.mark.database
async def test_e_a13_unknown_id_returns_json_404(
    api: AsyncClient, session: AsyncSession
) -> None:
    response = await api.get(f"{PAYMENTS}/{uuid.UUID(int=0)}")

    assert response.status_code == 404
    assert response.headers["Content-Type"] == "application/json"


@pytest.mark.database
async def test_created_response_is_json_with_utc_timestamp(
    api: AsyncClient, session: AsyncSession
) -> None:
    response = await post(api, BODY, key())

    assert response.headers["Content-Type"] == "application/json"
    assert response.json()["created_at"].endswith("Z")


# E-A14: database failures


def _db_error(kind: str) -> Exception:
    origin = Exception("server closed the connection unexpectedly")
    errors: dict[str, Exception] = {
        "operational": OperationalError("INSERT", {}, origin),
        "interface": InterfaceError("INSERT", {}, origin),
        "pool-timeout": PoolTimeoutError("QueuePool limit reached"),
        "invalidated": DBAPIError("INSERT", {}, origin, connection_invalidated=True),
        "integrity": IntegrityError("INSERT", {}, origin),
        "data": DataError("INSERT", {}, origin),
    }
    return errors[kind]


@pytest.mark.parametrize(
    ("kind", "expected_status"),
    [
        ("operational", 503),
        ("interface", 503),
        ("pool-timeout", 503),
        ("invalidated", 503),
        ("integrity", 500),
        ("data", 500),
    ],
)
async def test_e_a14_only_availability_errors_map_to_503(
    application: FastAPI,
    api: AsyncClient,
    async_engine: AsyncEngine,
    kind: str,
    expected_status: int,
) -> None:
    class BrokenSession(AsyncSession):
        async def _fail(self, *args: Any, **kwargs: Any) -> Any:
            raise _db_error(kind)

        execute = scalar = scalars = get = flush = commit = _fail

    factory = async_sessionmaker(async_engine, class_=BrokenSession)

    async def broken_session() -> AsyncIterator[AsyncSession]:
        async with factory() as own_session:
            yield own_session

    application.dependency_overrides[get_session] = broken_session

    response = await post(api, BODY, key())

    assert response.status_code == expected_status


@pytest.mark.database
async def test_e_a14_commit_failure_leaves_no_payment_or_outbox_row(
    application: FastAPI,
    api: AsyncClient,
    session: AsyncSession,
    async_engine: AsyncEngine,
) -> None:
    class CommitFailsSession(AsyncSession):
        async def commit(self) -> None:
            raise OperationalError("COMMIT", {}, Exception("connection lost"))

    factory = async_sessionmaker(async_engine, class_=CommitFailsSession)

    async def failing_session() -> AsyncIterator[AsyncSession]:
        async with factory() as own_session:
            yield own_session

    application.dependency_overrides[get_session] = failing_session

    response = await post(api, BODY, key())

    assert response.status_code == 503
    assert await count(session, Payment) == 0
    assert await count(session, OutboxEvent) == 0

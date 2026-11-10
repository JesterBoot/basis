import uuid
from collections.abc import AsyncIterator

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from httpx import AsyncClient

from core.config import Settings
from db.session import AsyncSession, get_session
from main import create_app
from tests.support.api import (
    PAYMENT_BODY,
    PAYMENTS_URL,
    TEST_API_KEY,
    new_idempotency_key,
)

ROUTES = pytest.mark.parametrize(
    ("method", "path"),
    [("POST", PAYMENTS_URL), ("GET", f"{PAYMENTS_URL}/{uuid.uuid4()}")],
    ids=["create", "get"],
)


@pytest.fixture
def session_entries(application: FastAPI) -> list[str]:
    entries: list[str] = []

    async def recording_session() -> AsyncIterator[AsyncSession]:
        entries.append("entered")
        raise HTTPException(status_code=418)
        yield

    application.dependency_overrides[get_session] = recording_session
    return entries


@pytest.mark.parametrize(
    "headers",
    [{}, {"X-API-Key": "wrong-key"}, {"X-API-Key": ""}],
    ids=["missing", "wrong", "empty"],
)
@ROUTES
async def test_api_routes_reject_missing_or_wrong_key(
    client: AsyncClient, method: str, path: str, headers: dict[str, str]
) -> None:
    client.headers.pop("X-API-Key")

    response = await client.request(method, path, headers=headers, json={})

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}


@ROUTES
async def test_e_a1_wrong_key_never_opens_db_session(
    client: AsyncClient, session_entries: list[str], method: str, path: str
) -> None:
    response = await client.request(
        method,
        path,
        json=PAYMENT_BODY,
        headers={"X-API-Key": "wrong-key", "Idempotency-Key": new_idempotency_key()},
    )

    assert response.status_code == 401
    assert session_entries == []


async def test_e_a1_valid_key_reaches_db_session(
    client: AsyncClient, session_entries: list[str]
) -> None:
    response = await client.get(f"{PAYMENTS_URL}/{uuid.uuid4()}")

    assert response.status_code == 418
    assert session_entries == ["entered"]


async def test_auth_runs_before_body_validation(client: AsyncClient) -> None:
    response = await client.post(
        PAYMENTS_URL,
        headers={"X-API-Key": "wrong-key"},
        json={"amount": "not-a-number"},
    )

    assert response.status_code == 401


async def test_valid_key_reaches_request_validation(client: AsyncClient) -> None:
    response = await client.post(PAYMENTS_URL, json={})

    assert response.status_code == 422


async def test_e_a1_header_name_is_case_insensitive(client: AsyncClient) -> None:
    client.headers.pop("X-API-Key")

    response = await client.post(
        PAYMENTS_URL, json={}, headers={"x-api-key": TEST_API_KEY}
    )

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
    client: AsyncClient, provided: str
) -> None:
    response = await client.post(PAYMENTS_URL, json={}, headers={"X-API-Key": provided})

    assert response.status_code == 401


async def test_e_a1_unauthorized_response_advertises_scheme(
    client: AsyncClient,
) -> None:
    client.headers.pop("X-API-Key")

    response = await client.post(PAYMENTS_URL, json={})

    assert response.headers["WWW-Authenticate"] == "APIKey"
    assert response.headers["Content-Type"] == "application/json"


async def test_health_endpoints_do_not_require_key(client: AsyncClient) -> None:
    client.headers.pop("X-API-Key")

    response = await client.get("/health/live")

    assert response.status_code == 200


def test_openapi_declares_api_key_security_scheme() -> None:
    with TestClient(create_app()) as test_client:
        schema = test_client.get("/openapi.json").json()

    assert schema["components"]["securitySchemes"]["APIKeyHeader"] == {
        "type": "apiKey",
        "in": "header",
        "name": "X-API-Key",
    }


def test_api_refuses_to_start_without_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_KEY")
    settings_without_key = Settings(_env_file=None)
    monkeypatch.setattr("core.setup.get_settings", lambda: settings_without_key)

    application = create_app()

    with (
        pytest.raises(RuntimeError, match="API_KEY must be set"),
        TestClient(application),
    ):
        pass


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

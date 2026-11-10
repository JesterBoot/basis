import uuid

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from core.config import Settings
from main import create_app


@pytest.mark.parametrize(
    "headers",
    [{}, {"X-API-Key": "wrong-key"}, {"X-API-Key": ""}],
    ids=["missing", "wrong", "empty"],
)
@pytest.mark.parametrize(
    ("method", "path"),
    [("POST", "/api/v1/payments"), ("GET", f"/api/v1/payments/{uuid.uuid4()}")],
    ids=["create", "get"],
)
async def test_api_routes_reject_missing_or_wrong_key(
    client: AsyncClient, method: str, path: str, headers: dict[str, str]
) -> None:
    client.headers.pop("X-API-Key")

    response = await client.request(method, path, headers=headers, json={})

    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key"}


async def test_auth_runs_before_body_validation(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        headers={"X-API-Key": "wrong-key"},
        json={"amount": "not-a-number"},
    )

    assert response.status_code == 401


async def test_valid_key_reaches_request_validation(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json={})

    assert response.status_code == 422


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

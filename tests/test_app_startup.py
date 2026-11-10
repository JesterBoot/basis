import pytest
from fastapi.testclient import TestClient

from core.config import Settings
from main import create_app


def test_application_imports_without_database_connection() -> None:
    application = create_app()
    client = TestClient(application)
    response = client.get("/openapi.json")
    docs_response = client.get("/docs")
    client.close()

    assert response.status_code == 200
    assert docs_response.status_code == 200
    assert response.json()["info"]["title"] == "Basis Backend API"
    assert response.json()["info"]["version"] == "0.1.0"
    assert "/health/live" in response.json()["paths"]


@pytest.mark.database
def test_health_checks_report_liveness_and_readiness() -> None:
    with TestClient(create_app()) as client:
        liveness_response = client.get("/health/live")
        readiness_response = client.get("/health/ready")
        startup_response = client.get("/health/startup")

    assert liveness_response.status_code == 200
    assert liveness_response.json()["status"] == "ok"
    assert readiness_response.status_code == 200
    assert readiness_response.json()["status"] == "ok"
    assert startup_response.status_code == 200
    assert startup_response.json()["status"] == "ok"


def test_startup_check_returns_service_unavailable_when_database_is_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def database_is_down() -> bool:
        return False

    monkeypatch.setattr("core.utils.check_conn_psql", database_is_down)

    with TestClient(create_app()) as client:
        response = client.get("/health/startup")

    assert response.status_code == 503
    assert response.json()["detail"] == "Service dependencies not ready"


def test_readiness_returns_service_unavailable_before_successful_startup() -> None:
    application = create_app()
    application.state.startup_ok = False

    client = TestClient(application)
    response = client.get("/health/ready")
    client.close()

    assert response.status_code == 503
    assert response.json()["detail"] == "not_ok"


def test_default_settings_are_bootstrap_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Validate application defaults independently from Docker Compose runtime settings.
    monkeypatch.delenv("DEBUG", raising=False)
    for variable in (
        "DATABASE_URL",
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        monkeypatch.delenv(variable, raising=False)

    settings = Settings(_env_file=None)

    assert settings.database_url == (
        "postgresql+psycopg://basis:basis@127.0.0.1:5432/basis"
    )
    assert settings.debug is False


def test_settings_can_be_overridden_by_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@db/basis_test")

    settings = Settings(_env_file=None)

    assert settings.database_url == "postgresql+psycopg://user:pass@db/basis_test"

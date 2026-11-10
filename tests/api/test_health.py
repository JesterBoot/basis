import pytest
from fastapi.testclient import TestClient

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

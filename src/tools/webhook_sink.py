import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request, Response
from pydantic import BaseModel, Field

from core.config import get_settings
from utils.signing import verify


class SinkConfig(BaseModel):
    status_code: int = Field(default=200, ge=100, le=599)
    fail_times: int = Field(default=0, ge=0)
    fail_status_code: int = Field(default=503, ge=100, le=599)
    delay_seconds: float = Field(default=0, ge=0, le=30)


class SinkCall(BaseModel):
    received_at: datetime
    event_id: str | None
    payment_id: str | None
    status: str | None
    signature_valid: bool
    duplicate: bool
    responded_with: int
    body: dict[str, Any] | None


class SinkState:
    def __init__(self) -> None:
        self.config = SinkConfig()
        self.calls: list[SinkCall] = []
        self._failures_left = 0

    def configure(self, config: SinkConfig) -> None:
        self.config = config
        self._failures_left = config.fail_times

    def reset(self) -> None:
        self.calls.clear()
        self._failures_left = self.config.fail_times

    def next_status_code(self) -> int:
        if self._failures_left > 0:
            self._failures_left -= 1
            return self.config.fail_status_code
        return self.config.status_code


def _parse(body: bytes) -> dict[str, Any] | None:
    try:
        parsed = json.loads(body)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def create_sink_app(secret: str | None = None) -> FastAPI:
    resolved_secret = secret
    if resolved_secret is None:
        configured = get_settings().webhook_secret
        resolved_secret = configured.get_secret_value() if configured else ""

    app = FastAPI(title="Webhook sink", version="1.0")
    state = SinkState()
    app.state.sink = state

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/hook")
    async def hook(request: Request) -> Response:
        body = await request.body()
        signature = request.headers.get("X-Webhook-Signature", "")
        event_id = request.headers.get("X-Event-Id")
        payload = _parse(body)
        status_code = state.next_status_code()
        duplicate = event_id is not None and any(
            call.event_id == event_id for call in state.calls
        )
        state.calls.append(
            SinkCall(
                received_at=datetime.now(UTC),
                event_id=event_id,
                payment_id=(payload or {}).get("payment_id"),
                status=(payload or {}).get("status"),
                signature_valid=bool(resolved_secret)
                and verify(resolved_secret, body, signature),
                duplicate=duplicate,
                responded_with=status_code,
                body=payload,
            )
        )
        if state.config.delay_seconds:
            await asyncio.sleep(state.config.delay_seconds)
        return Response(status_code=status_code)

    @app.get("/calls")
    async def list_calls(payment_id: str | None = None) -> list[SinkCall]:
        if payment_id is None:
            return state.calls
        return [call for call in state.calls if call.payment_id == payment_id]

    @app.delete("/calls", status_code=204)
    async def reset_calls() -> Response:
        state.reset()
        return Response(status_code=204)

    @app.get("/config")
    async def get_config() -> SinkConfig:
        return state.config

    @app.post("/config")
    async def set_config(config: SinkConfig) -> SinkConfig:
        state.configure(config)
        return state.config

    return app


app = create_sink_app()

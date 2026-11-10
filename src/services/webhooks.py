import uuid
from collections.abc import Awaitable, Callable
from types import TracebackType
from typing import Self

import httpx

from core.config import Settings, get_settings
from core.custom_logging import get_logger
from db.session import AsyncSession
from models import Payment, WebhookStatus
from schemas.payment import PaymentRead
from schemas.webhooks import WebhookPayload
from services.base import BaseService
from services.exceptions import WebhookPermanentError, WebhookTransientError
from services.payments import PaymentDataManager
from utils.network import IPAddress, is_private_address, is_private_host, resolve_host
from utils.signing import sign

logger = get_logger()

Resolver = Callable[[str, int], Awaitable[list[IPAddress]]]


class WebhookClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        settings: Settings | None = None,
        resolver: Resolver = resolve_host,
    ) -> None:
        self.settings = settings or get_settings()
        self.secret = self._require_secret(self.settings)
        self.http = http
        self.resolver = resolver

    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> Self:
        resolved = settings or get_settings()
        cls._require_secret(resolved)
        http = httpx.AsyncClient(
            timeout=resolved.webhook_timeout,
            follow_redirects=False,
            headers={"User-Agent": resolved.webhook_user_agent},
        )
        return cls(http, resolved)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.http.aclose()

    @staticmethod
    def _require_secret(settings: Settings) -> str:
        secret = settings.webhook_secret
        if secret is None or not secret.get_secret_value().strip():
            raise RuntimeError("WEBHOOK_SECRET must be set to deliver webhooks")
        return secret.get_secret_value()

    async def send(self, url: str, payload: WebhookPayload) -> None:
        await self._ensure_public_destination(url)
        body = payload.model_dump_json().encode()
        headers = {
            "Content-Type": "application/json",
            "X-Event-Id": str(payload.event_id),
            "X-Webhook-Signature": sign(self.secret, body),
        }
        try:
            async with self.http.stream(
                "POST", url, content=body, headers=headers
            ) as response:
                status_code = response.status_code
        except httpx.TimeoutException as exc:
            raise WebhookTransientError(f"Timeout: {type(exc).__name__}") from exc
        except httpx.TransportError as exc:
            raise WebhookTransientError(
                f"Transport error: {type(exc).__name__}"
            ) from exc
        self._raise_for_status(status_code)

    @staticmethod
    def _raise_for_status(status_code: int) -> None:
        if 200 <= status_code < 300:
            return
        if status_code in (408, 429) or status_code >= 500:
            raise WebhookTransientError(f"HTTP {status_code}")
        raise WebhookPermanentError(f"HTTP {status_code}")

    async def _ensure_public_destination(self, url: str) -> None:
        if self.settings.webhook_allow_private:
            return
        parsed = httpx.URL(url)
        if is_private_host(parsed.host):
            raise WebhookPermanentError(f"Webhook host {parsed.host} is private")
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            addresses = await self.resolver(parsed.host, port)
        except OSError as exc:
            raise WebhookTransientError(f"DNS lookup failed: {exc}") from exc
        private = [address for address in addresses if is_private_address(address)]
        if private:
            raise WebhookPermanentError(
                f"Webhook host {parsed.host} resolves to private address {private[0]}"
            )


class WebhookService(BaseService[Payment]):
    def __init__(
        self,
        session: AsyncSession,
        client: WebhookClient,
        settings: Settings | None = None,
    ) -> None:
        super().__init__(session)
        self.data_manager = PaymentDataManager(session)
        self.client = client
        self.settings = settings or get_settings()

    async def deliver(self, payment: PaymentRead, event_id: uuid.UUID) -> None:
        payload = WebhookPayload(
            event_id=event_id,
            payment_id=payment.payment_id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            processed_at=payment.processed_at,
        )
        try:
            await self.client.send(str(payment.webhook_url), payload)
        except WebhookPermanentError as exc:
            attempts = await self._record(payment.payment_id, WebhookStatus.FAILED, exc)
            logger.warning(
                "Webhook rejected permanently",
                **self._fields(payment, event_id, attempts, exc),
            )
            raise
        except WebhookTransientError as exc:
            attempts = await self._record(payment.payment_id, None, exc)
            logger.warning(
                "Webhook delivery failed, will retry",
                **self._fields(payment, event_id, attempts, exc),
            )
            raise
        attempts = await self._record(payment.payment_id, WebhookStatus.DELIVERED, None)
        logger.info(
            "Webhook delivered", **self._fields(payment, event_id, attempts, None)
        )

    async def mark_failed(self, payment_id: uuid.UUID) -> None:
        async with self.session.transaction():
            await self.data_manager.mark_webhook_failed(payment_id)
        logger.warning("Webhook delivery gave up", payment_id=str(payment_id))

    async def _record(
        self,
        payment_id: uuid.UUID,
        status: WebhookStatus | None,
        error: Exception | None,
    ) -> int:
        message = None
        if error is not None:
            message = str(error)[: self.settings.webhook_last_error_max_length]
        async with self.session.transaction():
            return await self.data_manager.record_webhook_attempt(
                payment_id, status=status, error=message
            )

    @staticmethod
    def _fields(
        payment: PaymentRead,
        event_id: uuid.UUID,
        attempts: int,
        error: Exception | None,
    ) -> dict[str, object]:
        return {
            "payment_id": str(payment.payment_id),
            "event_id": str(event_id),
            "attempts": attempts,
            "error": str(error) if error is not None else None,
        }

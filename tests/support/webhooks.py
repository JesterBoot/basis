import ipaddress
import uuid
from decimal import Decimal

from core.config import Settings
from models import Currency, PaymentStatus
from schemas.webhooks import WebhookPayload
from utils.network import IPAddress

WEBHOOK_URL = "https://merchant.example/hooks/payments"
WEBHOOK_SECRET = "test-webhook-secret"


def webhook_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "WEBHOOK_SECRET": WEBHOOK_SECRET,
        "WEBHOOK_ALLOW_PRIVATE": False,
        "WEBHOOK_TIMEOUT": 1,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


async def public_resolver(host: str, port: int) -> list[IPAddress]:
    return [ipaddress.ip_address("93.184.216.34")]


def make_payload(**overrides: object) -> WebhookPayload:
    values: dict[str, object] = {
        "event_id": uuid.uuid4(),
        "payment_id": uuid.uuid4(),
        "status": PaymentStatus.SUCCEEDED,
        "amount": Decimal("100.50"),
        "currency": Currency.USD,
        "processed_at": None,
    }
    values.update(overrides)
    return WebhookPayload.model_validate(values)

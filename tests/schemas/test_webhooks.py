import json
from decimal import Decimal

from tests.support.webhooks import make_payload


def test_payload_serializes_amount_as_string() -> None:
    payload = make_payload(amount=Decimal("10.00"))

    assert json.loads(payload.model_dump_json())["amount"] == "10.00"

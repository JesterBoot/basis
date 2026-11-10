import json
import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from messaging.names import Exchange, Header, Queue, RoutingKey
from models import AggregateType, EventType
from schemas.events import PaymentCreatedEvent


def make_event(**overrides: object) -> PaymentCreatedEvent:
    values: dict[str, object] = {
        "event_id": uuid.uuid4(),
        "payment_id": uuid.uuid4(),
        "occurred_at": datetime(2026, 11, 10, 12, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return PaymentCreatedEvent.model_validate(values)


def test_event_type_defaults_to_payment_created() -> None:
    assert make_event().event_type is EventType.PAYMENT_CREATED


def test_event_serializes_to_plain_json() -> None:
    event = make_event()

    data = json.loads(event.model_dump_json())

    assert data == {
        "event_id": str(event.event_id),
        "event_type": "payment.created",
        "payment_id": str(event.payment_id),
        "occurred_at": "2026-11-10T12:00:00Z",
    }


def test_event_round_trips_from_message_body() -> None:
    event = make_event()

    assert PaymentCreatedEvent.model_validate_json(event.model_dump_json()) == event


@pytest.mark.parametrize("event_type", ["payment.updated", "PAYMENT.CREATED", ""])
def test_event_rejects_other_event_types(event_type: str) -> None:
    with pytest.raises(ValidationError):
        make_event(event_type=event_type)


@pytest.mark.parametrize("field", ["event_id", "payment_id"])
def test_event_requires_valid_uuids(field: str) -> None:
    with pytest.raises(ValidationError):
        make_event(**{field: "not-a-uuid"})


def test_enum_values_match_spec() -> None:
    assert AggregateType.PAYMENT == "payment"
    assert EventType.PAYMENT_CREATED == "payment.created"
    assert Exchange.PAYMENTS == "payments"
    assert Exchange.PAYMENTS_DLX == "payments.dlx"
    assert Queue.PAYMENTS_NEW == "payments.new"
    assert Queue.PAYMENTS_DLQ == "payments.new.dlq"
    assert RoutingKey.PAYMENTS_NEW == "payments.new"
    assert RoutingKey.PAYMENTS_DLQ == "payments.new.dlq"
    assert Header.ATTEMPT == "x-attempt"


def test_messaging_names_are_plain_strings() -> None:
    assert isinstance(Queue.PAYMENTS_NEW, str)
    assert f"{RoutingKey.PAYMENTS_NEW}" == "payments.new"

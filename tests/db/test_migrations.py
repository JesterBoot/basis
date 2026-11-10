from collections.abc import Iterator

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine, create_engine, inspect

import models  # noqa: F401
from core.config import Settings
from db.base import Base
from tests.support.database import alembic_config, build_test_database_url

DATABASE_URL = build_test_database_url(Settings().database_url)

pytestmark = pytest.mark.database


@pytest.fixture
def sync_engine() -> Iterator[Engine]:
    engine = create_engine(DATABASE_URL)
    yield engine
    engine.dispose()


def test_upgrade_creates_payments_and_outbox(sync_engine: Engine) -> None:
    inspector = inspect(sync_engine)

    assert {"payments", "outbox"} <= set(inspector.get_table_names())
    assert {column["name"] for column in inspector.get_columns("payments")} == {
        "id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "idempotency_key",
        "request_hash",
        "webhook_url",
        "webhook_status",
        "webhook_attempts",
        "webhook_last_error",
        "webhook_delivered_at",
        "created_at",
        "updated_at",
        "processed_at",
    }
    assert {check["name"] for check in inspector.get_check_constraints("payments")} == {
        "ck_payments_amount_positive",
        "ck_payments_currency",
        "ck_payments_status",
        "ck_payments_webhook_status",
        "ck_payments_webhook_attempts_non_negative",
    }
    assert [
        constraint["column_names"]
        for constraint in inspector.get_unique_constraints("payments")
    ] == [["idempotency_key"]]


def test_outbox_has_partial_index_on_unpublished_rows(sync_engine: Engine) -> None:
    indexes = {
        index["name"]: index for index in inspect(sync_engine).get_indexes("outbox")
    }

    unpublished = indexes["ix_outbox_unpublished"]
    assert unpublished["column_names"] == ["next_attempt_at"]
    assert (
        unpublished["dialect_options"]["postgresql_where"] == "(published_at IS NULL)"
    )
    assert "ix_outbox_aggregate_id" in indexes


def test_models_match_migrations(sync_engine: Engine) -> None:
    with sync_engine.connect() as connection:
        context = MigrationContext.configure(connection, opts={"compare_type": True})
        diff = compare_metadata(context, Base.metadata)

    assert diff == []


def test_downgrade_and_upgrade_round_trip(sync_engine: Engine) -> None:
    config = alembic_config(DATABASE_URL)
    try:
        command.downgrade(config, "base")
        assert not {"payments", "outbox"} & set(inspect(sync_engine).get_table_names())
    finally:
        command.upgrade(config, "head")

    assert {"payments", "outbox"} <= set(inspect(sync_engine).get_table_names())

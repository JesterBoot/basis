import pytest
from sqlalchemy import text

from core.config import Settings
from db import session as database_session
from db.session import (
    AsyncSession,
    build_async_engine,
    check_conn_psql,
    tasks_session_context,
)


@pytest.mark.database
async def test_tasks_session_context_yields_working_session() -> None:
    async with tasks_session_context() as session:
        assert isinstance(session, AsyncSession)
        assert await session.scalar(text("SELECT 1")) == 1


@pytest.mark.database
async def test_check_conn_psql_reports_available_database() -> None:
    assert await check_conn_psql() is True


async def test_check_conn_psql_reports_unreachable_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unreachable = build_async_engine(
        Settings(
            _env_file=None,
            DATABASE_URL="postgresql+psycopg://basis:basis@127.0.0.1:1/basis",
        )
    )
    monkeypatch.setattr(database_session, "engine", unreachable)
    try:
        assert await check_conn_psql() is False
    finally:
        await unreachable.dispose()


@pytest.mark.database
async def test_transaction_commits_on_success_and_rolls_back_on_error() -> None:
    async with tasks_session_context() as session:
        async with session.transaction():
            await session.execute(
                text("CREATE TEMP TABLE tx_probe (n int) ON COMMIT PRESERVE ROWS")
            )
            await session.execute(text("INSERT INTO tx_probe VALUES (1)"))

        with pytest.raises(RuntimeError):
            async with session.transaction():
                await session.execute(text("INSERT INTO tx_probe VALUES (2)"))
                raise RuntimeError("boom")

        assert await session.scalar(text("SELECT array_agg(n) FROM tx_probe")) == [1]

from pathlib import Path

from alembic.config import Config
from psycopg import Error as PsycopgError
from psycopg import connect
from psycopg.sql import SQL, Identifier
from sqlalchemy import func, select
from sqlalchemy.engine import make_url

from db.session import AsyncSession
from models import OutboxEvent, Payment

TEST_DATABASE_SUFFIX = "_test"
ALEMBIC_INI = Path(__file__).resolve().parents[2] / "src" / "alembic.ini"


def build_test_database_url(database_url: str) -> str:
    parsed_url = make_url(database_url)
    if parsed_url.get_backend_name() != "postgresql":
        return database_url

    database_name = parsed_url.database
    if not database_name:
        raise ValueError("PostgreSQL DATABASE_URL must include a database name")

    if not database_name.endswith(TEST_DATABASE_SUFFIX):
        database_name = f"{database_name}{TEST_DATABASE_SUFFIX}"

    return parsed_url.set(database=database_name).render_as_string(
        hide_password=False,
    )


def database_name(database_url: str) -> str:
    database = make_url(database_url).database
    if not database:
        raise ValueError("PostgreSQL DATABASE_URL must include a database name")
    return database


def _admin_connection_url(database_url: str) -> str:
    parsed_url = make_url(database_url)
    return parsed_url.set(
        database="postgres",
        drivername="postgresql",
    ).render_as_string(hide_password=False)


def postgres_server_available(database_url: str) -> bool:
    if make_url(database_url).get_backend_name() != "postgresql":
        return False

    try:
        with connect(_admin_connection_url(database_url), connect_timeout=2):
            return True
    except PsycopgError:
        return False


def _database_exists(database_url: str, target_database: str) -> bool:
    with connect(_admin_connection_url(database_url)) as connection:
        result = connection.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (target_database,),
        )
        return result.fetchone() is not None


def ensure_test_database(database_url: str) -> None:
    test_database_url = build_test_database_url(database_url)
    test_database = database_name(test_database_url)
    base_database = database_name(database_url)
    if test_database == base_database:
        raise ValueError("Test database must differ from the configured database")

    with connect(
        _admin_connection_url(database_url),
        autocommit=True,
    ) as connection:
        if not _database_exists(database_url, test_database):
            connection.execute(
                SQL("CREATE DATABASE {}").format(
                    Identifier(test_database),
                ),
            )


def alembic_config(database_url: str) -> Config:
    config = Config(str(ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    return config


async def count_rows(
    session: AsyncSession, model: type[Payment] | type[OutboxEvent]
) -> int:
    return await session.scalar(select(func.count()).select_from(model)) or 0

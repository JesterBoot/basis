from structlog import get_logger

from db.session import check_conn_psql

logger = get_logger()


async def check_startup_dependencies() -> bool:
    database_ok = await check_conn_psql()
    if not database_ok:
        logger.error("Startup dependency check failed")
    # TODO add rabbit check
    return database_ok

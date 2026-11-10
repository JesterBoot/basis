from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from structlog import get_logger

logger = get_logger()

DATABASE_UNAVAILABLE_ERRORS: tuple[type[Exception], ...] = (
    OperationalError,
    InterfaceError,
    PoolTimeoutError,
)


async def database_unavailable_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    logger.error(
        "Database unavailable",
        path=request.url.path,
        error=type(exc).__name__,
    )
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={"detail": "Database unavailable"},
        headers={"Retry-After": "5"},
    )


async def dbapi_error_handler(request: Request, exc: Exception) -> JSONResponse:
    if isinstance(exc, DBAPIError) and exc.connection_invalidated:
        return await database_unavailable_handler(request, exc)
    raise exc


async def request_validation_handler(request: Request, exc: Exception) -> JSONResponse:
    errors = exc.errors() if isinstance(exc, RequestValidationError) else []
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content={
            "detail": jsonable_encoder(
                [{k: v for k, v in error.items() if k != "input"} for error in errors]
            )
        },
    )


def register_error_handlers(application: FastAPI) -> None:
    application.add_exception_handler(
        RequestValidationError, request_validation_handler
    )
    for error_class in DATABASE_UNAVAILABLE_ERRORS:
        application.add_exception_handler(error_class, database_unavailable_handler)
    application.add_exception_handler(DBAPIError, dbapi_error_handler)

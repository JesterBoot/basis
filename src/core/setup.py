from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.errors import register_error_handlers
from api.router import api_router
from core.config import get_settings
from core.custom_logging import configure_logging
from core.utils import check_startup_dependencies

configure_logging()


def create_app() -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        api_key = get_settings().api_key
        if api_key is None or not api_key.get_secret_value().strip():
            raise RuntimeError("API_KEY must be set to start the API")
        application.state.startup_ok = await check_startup_dependencies()
        yield

    application = FastAPI(
        title="Basis Backend API",
        version="0.1.0",
        debug=settings.debug,
        lifespan=lifespan,
    )
    application.state.startup_ok = False
    application.state.settings = settings
    application.include_router(api_router)
    register_error_handlers(application)
    return application

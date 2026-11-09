from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.router import api_router
from core.config import get_settings
from core.custom_logging import configure_logging
from core.utils import check_startup_dependencies

configure_logging()


def create_app() -> FastAPI:
    settings = get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
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
    return application

import os

import uvicorn

from core.config import get_settings
from core.setup import create_app

app = create_app()


if __name__ == "__main__":
    settings = get_settings()
    uvicorn.run(
        "main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_CONTAINER_PORT", "8080")),
        reload=settings.debug,
    )

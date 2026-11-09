#!/bin/sh

set -eu

API_HOST="${API_HOST:-0.0.0.0}"
API_CONTAINER_PORT="${API_CONTAINER_PORT:-8080}"
DEBUG="${DEBUG:-0}"

/app/.venv/bin/alembic \
    -c /app/src/alembic.ini \
    upgrade head

if [ "${DEBUG}" = "1" ]; then
    exec /app/.venv/bin/uvicorn \
        main:app \
        --host "${API_HOST}" \
        --port "${API_CONTAINER_PORT}" \
        --reload \
        --reload-dir /app/src
fi

exec /app/.venv/bin/uvicorn \
    main:app \
    --host "${API_HOST}" \
    --port "${API_CONTAINER_PORT}"

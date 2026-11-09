FROM python:3.13-slim AS builder

ARG UV_VERSION=0.7.20

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_COMPILE_BYTECODE=0 \
    UV_LINK_MODE=copy \
    PYTHONPATH=/app/src

WORKDIR /app

RUN python -m pip install --upgrade pip \
    && python -m pip install "uv==${UV_VERSION}"

COPY pyproject.toml uv.lock ./

RUN uv sync --locked --no-dev --no-install-project \
    && find /app/.venv -type d -name __pycache__ -prune -exec rm -rf {} + \
    && find /app/.venv -type f -name '*.py[co]' -delete

FROM python:3.13-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY scripts ./scripts

EXPOSE 8080

ENTRYPOINT ["sh", "/app/scripts/entrypoint.sh"]

.PHONY: install check-fix format sort-imports test test-all test-db \
	check run migrate downgrade-migration make-migration show-heads \
	docker-config docker-build docker-up docker-down docker-logs

UV ?= uv
DC ?= docker compose
API_SERVICE ?= api
ALEMBIC ?= $(UV) run alembic
ALEMBIC_CONFIG ?= src/alembic.ini

install:
	$(UV) sync

check-fix:
	cd src && uv run ruff check . --fix

format:
	cd src && uv run ruff format .

sort-imports:
	cd src && uv run ruff check . --fix --select I

test-all:
	$(UV) run pytest

test-db:
	$(UV) run pytest tests/test_database.py

check:
	$(UV) lock --check
	$(UV) run ruff check src tests scripts
	$(MAKE) test

run:
	$(DC) up -d --build

migrate:
	$(DC) exec $(API_SERVICE) /app/.venv/bin/alembic \
		-c /app/src/alembic.ini \
		upgrade head

downgrade-migration:
	$(DC) exec $(API_SERVICE) /app/.venv/bin/alembic \
		-c /app/src/alembic.ini \
		downgrade -1

make-migration:
	$(ALEMBIC) -c $(ALEMBIC_CONFIG) \
		revision --autogenerate -m "$(or $(name),migration)"

show-heads:
	$(ALEMBIC) -c $(ALEMBIC_CONFIG) heads

docker-config:
	$(DC) config

docker-build:
	$(DC) build

docker-up:
	$(DC) up -d --build

docker-down:
	$(DC) down

docker-logs:
	$(DC) logs -f $(API_SERVICE)

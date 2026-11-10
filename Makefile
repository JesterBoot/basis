.PHONY: install check-fix format sort-imports test test-all test-db \
	check run migrate downgrade-migration make-migration show-heads \
	docker-config docker-build docker-up docker-down docker-logs \
	e2e chaos dlq-list dlq-replay test-stack-down

UV ?= uv
DC ?= docker compose -f docker-compose.yml -f docker-compose.dev.yml
DC_TEST ?= docker compose -p basis-test -f docker-compose.yml -f docker-compose.test.yml
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

test:
	$(UV) run pytest

test-all: test

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

e2e:
	bash scripts/e2e.sh

chaos:
	bash scripts/chaos.sh $(scenarios)

dlq-list:
	$(DC) exec consumer /app/.venv/bin/python -m tools.dlq_replay --dry-run --limit $(or $(limit),100)

dlq-replay:
	$(DC) exec consumer /app/.venv/bin/python -m tools.dlq_replay --limit $(or $(limit),100)

test-stack-down:
	$(DC_TEST) down -v --remove-orphans

from pathlib import Path

import pytest
from alembic.config import Config

from core.config import Settings
from tests.support.database import build_test_database_url

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_URL = build_test_database_url(Settings().database_url)

pytestmark = [
    pytest.mark.database,
    pytest.mark.skipif(
        not DATABASE_URL.startswith("postgresql")
    ),
]


def _alembic_config() -> Config:
    config = Config(str(PROJECT_ROOT / "src" / "alembic.ini"))
    config.set_main_option("sqlalchemy.url", DATABASE_URL.replace("%", "%%"))
    return config

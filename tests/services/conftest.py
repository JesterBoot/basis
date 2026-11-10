from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from services.webhooks import WebhookClient
from tests.support.webhooks import public_resolver, webhook_settings


@pytest_asyncio.fixture
async def webhook_http() -> AsyncIterator[httpx.AsyncClient]:
    async with WebhookClient.from_settings(webhook_settings()) as webhook_client:
        yield webhook_client.http


@pytest.fixture
def webhook_client(webhook_http: httpx.AsyncClient) -> WebhookClient:
    return WebhookClient(webhook_http, webhook_settings(), resolver=public_resolver)

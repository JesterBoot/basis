import os
from urllib.parse import quote

import aio_pika
import httpx

from core.config import Settings

TEST_VHOST = "basis_test"
TEST_RETRY_BASE_DELAY_MS = 300


def management_url(settings: Settings) -> str:
    port = os.environ.get("RABBITMQ_MANAGEMENT_PORT", "15672")
    return f"http://{settings.rabbitmq_host}:{port}/api"


def _client(settings: Settings) -> httpx.Client:
    return httpx.Client(
        base_url=management_url(settings),
        auth=(settings.rabbitmq_user, settings.rabbitmq_password),
        timeout=5,
    )


def rabbitmq_available(settings: Settings) -> bool:
    try:
        with _client(settings) as client:
            return client.get("/overview").status_code == 200
    except httpx.HTTPError:
        return False


def create_vhost(settings: Settings, vhost: str) -> None:
    encoded = quote(vhost, safe="")
    with _client(settings) as client:
        client.put(f"/vhosts/{encoded}").raise_for_status()
        client.put(
            f"/permissions/{encoded}/{quote(settings.rabbitmq_user, safe='')}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        ).raise_for_status()


def delete_vhost(settings: Settings, vhost: str) -> None:
    with _client(settings) as client:
        response = client.delete(f"/vhosts/{quote(vhost, safe='')}")
        if response.status_code not in (204, 404):
            response.raise_for_status()


def list_queues(settings: Settings, vhost: str) -> dict[str, dict]:
    with _client(settings) as client:
        response = client.get(f"/queues/{quote(vhost, safe='')}")
        response.raise_for_status()
        return {queue["name"]: queue for queue in response.json()}


def list_bindings(settings: Settings, vhost: str) -> set[tuple[str, str, str]]:
    with _client(settings) as client:
        response = client.get(f"/bindings/{quote(vhost, safe='')}")
        response.raise_for_status()
        return {
            (binding["source"], binding["routing_key"], binding["destination"])
            for binding in response.json()
            if binding["source"]
        }


async def count_messages(channel: aio_pika.abc.AbstractChannel, queue_name: str) -> int:
    queue = await channel.get_queue(queue_name)
    held = []
    while (message := await queue.get(no_ack=False, fail=False)) is not None:
        held.append(message)
    for message in held:
        await message.nack(requeue=True)
    return len(held)

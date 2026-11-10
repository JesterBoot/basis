import asyncio
import json

import aio_pika
import pytest
from aio_pika.exceptions import DeliveryError
from aiormq.exceptions import ChannelPreconditionFailed
from faststream.rabbit import RabbitBroker

from core.config import Settings
from messaging.broker import build_broker
from messaging.names import Exchange, Queue, RoutingKey, retry_queue_name
from messaging.topology import PaymentsTopology
from tests.support.rabbitmq import (
    TEST_RETRY_BASE_DELAY_MS,
    TEST_VHOST,
    list_bindings,
    list_queues,
)

pytestmark = pytest.mark.rabbitmq


async def get_message(
    channel: aio_pika.abc.AbstractChannel, queue_name: str, timeout: float = 3.0
) -> aio_pika.abc.AbstractIncomingMessage | None:
    queue = await channel.get_queue(queue_name)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        message = await queue.get(no_ack=False, fail=False)
        if message is not None:
            return message
        await asyncio.sleep(0.05)
    return None


async def test_topology_declares_expected_queues(
    broker: RabbitBroker, rabbitmq_settings: Settings
) -> None:
    queues = list_queues(rabbitmq_settings, TEST_VHOST)

    assert set(queues) == {
        Queue.PAYMENTS_NEW,
        "payments.new.retry.300ms",
        "payments.new.retry.600ms",
        Queue.PAYMENTS_DLQ,
    }
    assert all(queue["durable"] for queue in queues.values())
    assert queues[Queue.PAYMENTS_NEW]["arguments"] == {
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new.dlq",
        "x-queue-type": "classic",
    }
    for ttl in (TEST_RETRY_BASE_DELAY_MS, TEST_RETRY_BASE_DELAY_MS * 2):
        assert queues[retry_queue_name(ttl)]["arguments"] == {
            "x-message-ttl": ttl,
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": "payments.new",
            "x-queue-type": "classic",
        }


async def test_topology_declares_expected_bindings(
    broker: RabbitBroker, rabbitmq_settings: Settings
) -> None:
    assert list_bindings(rabbitmq_settings, TEST_VHOST) == {
        ("payments", "payments.new", "payments.new"),
        ("payments", "payments.new.retry.300ms", "payments.new.retry.300ms"),
        ("payments", "payments.new.retry.600ms", "payments.new.retry.600ms"),
        ("payments.dlx", "payments.new.dlq", "payments.new.dlq"),
    }


async def test_declare_topology_is_idempotent(
    broker: RabbitBroker, rabbitmq_settings: Settings
) -> None:
    await PaymentsTopology(rabbitmq_settings).declare(broker)

    assert len(list_queues(rabbitmq_settings, TEST_VHOST)) == 4


async def test_message_routes_to_payments_new(
    broker: RabbitBroker, amqp_channel: aio_pika.abc.AbstractChannel
) -> None:
    await broker.publish(
        {"hello": "world"},
        exchange=Exchange.PAYMENTS,
        routing_key=RoutingKey.PAYMENTS_NEW,
    )

    message = await get_message(amqp_channel, Queue.PAYMENTS_NEW)

    assert message is not None
    assert json.loads(message.body) == {"hello": "world"}
    await message.ack()


async def test_retry_queue_dead_letters_back_after_ttl(
    broker: RabbitBroker,
    amqp_channel: aio_pika.abc.AbstractChannel,
    rabbitmq_settings: Settings,
) -> None:
    await broker.publish(
        {"retry": 1},
        exchange=Exchange.PAYMENTS,
        routing_key=PaymentsTopology(rabbitmq_settings).retry_queue_for_attempt(1).name,
    )

    assert await get_message(amqp_channel, Queue.PAYMENTS_NEW, timeout=0.1) is None
    message = await get_message(amqp_channel, Queue.PAYMENTS_NEW, timeout=3.0)

    assert message is not None
    assert json.loads(message.body) == {"retry": 1}
    assert message.headers["x-death"][0]["queue"] == "payments.new.retry.300ms"
    await message.ack()


async def test_rejected_message_goes_to_dead_letter_queue(
    broker: RabbitBroker, amqp_channel: aio_pika.abc.AbstractChannel
) -> None:
    await broker.publish(
        {"poison": True},
        exchange=Exchange.PAYMENTS,
        routing_key=RoutingKey.PAYMENTS_NEW,
    )
    message = await get_message(amqp_channel, Queue.PAYMENTS_NEW)
    assert message is not None

    await message.reject(requeue=False)
    dead = await get_message(amqp_channel, Queue.PAYMENTS_DLQ)

    assert dead is not None
    assert json.loads(dead.body) == {"poison": True}
    assert dead.headers["x-death"][0]["reason"] == "rejected"
    await dead.ack()


async def test_unroutable_publish_raises(broker: RabbitBroker) -> None:
    with pytest.raises(DeliveryError):
        await broker.publish(
            {"lost": True}, exchange=Exchange.PAYMENTS, routing_key="no.such.queue"
        )


async def test_changed_retry_delay_declares_new_queues_instead_of_failing(
    broker: RabbitBroker,
    rabbitmq_settings: Settings,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    changed = rabbitmq_settings.model_copy(update={"retry_base_delay_ms": 250})
    other_broker = build_broker(changed)
    await other_broker.connect()
    try:
        await PaymentsTopology(changed).declare(other_broker)
        queues = list_queues(rabbitmq_settings, TEST_VHOST)
        assert {"payments.new.retry.250ms", "payments.new.retry.500ms"} <= set(queues)
    finally:
        await other_broker.stop()
        for name in ("payments.new.retry.250ms", "payments.new.retry.500ms"):
            await amqp_channel.queue_delete(name)


def test_retry_queue_names_carry_their_delay() -> None:
    settings = Settings(_env_file=None, RETRY_BASE_DELAY_MS=2000, RETRY_MAX_ATTEMPTS=3)

    topology = PaymentsTopology(settings)

    assert topology.retry_queue_for_attempt(1).name == "payments.new.retry.2000ms"
    assert topology.retry_queue_for_attempt(2).name == "payments.new.retry.4000ms"
    assert [queue.name for queue in topology.queues] == [
        "payments.new",
        "payments.new.retry.2000ms",
        "payments.new.retry.4000ms",
        "payments.new.dlq",
    ]


async def test_conflicting_queue_arguments_fail_loudly(
    broker: RabbitBroker,
    rabbitmq_settings: Settings,
    amqp_channel: aio_pika.abc.AbstractChannel,
) -> None:
    await amqp_channel.declare_queue(
        "payments.new.retry.999ms", durable=True, arguments={"x-message-ttl": 1}
    )
    changed = rabbitmq_settings.model_copy(
        update={"retry_base_delay_ms": 999, "retry_max_attempts": 2}
    )
    other_broker = build_broker(changed)
    await other_broker.connect()
    try:
        with pytest.raises(ChannelPreconditionFailed):
            await PaymentsTopology(changed).declare(other_broker)
    finally:
        await other_broker.stop()
        await amqp_channel.queue_delete("payments.new.retry.999ms")

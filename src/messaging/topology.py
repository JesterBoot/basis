from aio_pika import RobustQueue
from aiormq.exceptions import ChannelPreconditionFailed
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from core.config import Settings
from core.custom_logging import get_logger
from messaging.names import Exchange, Queue, retry_queue_name

logger = get_logger()


class PaymentsTopology:
    exchange = RabbitExchange(Exchange.PAYMENTS, type=ExchangeType.DIRECT, durable=True)
    dead_letter_exchange = RabbitExchange(
        Exchange.PAYMENTS_DLX, type=ExchangeType.DIRECT, durable=True
    )
    exchanges = (exchange, dead_letter_exchange)

    def __init__(self, settings: Settings) -> None:
        self.new_queue = self._durable_queue(
            Queue.PAYMENTS_NEW,
            dead_letter_exchange=self.dead_letter_exchange,
            dead_letter_routing_key=Queue.PAYMENTS_DLQ,
        )
        self.retry_queues = [
            self._durable_queue(
                retry_queue_name(delay_ms),
                dead_letter_exchange=self.exchange,
                dead_letter_routing_key=Queue.PAYMENTS_NEW,
                ttl_ms=delay_ms,
            )
            for delay_ms in settings.retry_delays_ms
        ]
        self.dead_letter_queue = self._durable_queue(Queue.PAYMENTS_DLQ)
        self.bindings = [
            (self.new_queue, self.exchange),
            *((queue, self.exchange) for queue in self.retry_queues),
            (self.dead_letter_queue, self.dead_letter_exchange),
        ]

    @property
    def queues(self) -> list[RabbitQueue]:
        return [queue for queue, _ in self.bindings]

    def retry_queue_for_attempt(self, attempt: int) -> RabbitQueue:
        return self.retry_queues[attempt - 1]

    async def declare(self, broker: RabbitBroker) -> None:
        declared_exchanges = {
            exchange.name: await broker.declare_exchange(exchange)
            for exchange in self.exchanges
        }
        for queue, exchange in self.bindings:
            declared = await self._declare_queue(broker, queue)
            await declared.bind(
                declared_exchanges[exchange.name], routing_key=queue.routing_key
            )

    @staticmethod
    def _durable_queue(
        name: str,
        *,
        dead_letter_exchange: RabbitExchange | None = None,
        dead_letter_routing_key: str | None = None,
        ttl_ms: int | None = None,
    ) -> RabbitQueue:
        arguments: dict[str, str | int] = {}
        if dead_letter_exchange is not None and dead_letter_routing_key is not None:
            arguments["x-dead-letter-exchange"] = dead_letter_exchange.name
            arguments["x-dead-letter-routing-key"] = dead_letter_routing_key
        if ttl_ms is not None:
            arguments["x-message-ttl"] = ttl_ms
        return RabbitQueue(name, durable=True, routing_key=name, arguments=arguments)

    @staticmethod
    async def _declare_queue(broker: RabbitBroker, queue: RabbitQueue) -> RobustQueue:
        try:
            return await broker.declare_queue(queue)
        except ChannelPreconditionFailed:
            logger.error(
                "Queue exists with different arguments; delete it to redeclare",
                queue=queue.name,
                fix=f"rabbitmqctl delete_queue {queue.name}",
            )
            raise

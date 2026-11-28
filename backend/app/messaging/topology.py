"""Shared RabbitMQ topology for order integration events."""

EXCHANGE_NAME = "flowforge.events"
EXCHANGE_TYPE = "direct"
ORDERS_QUEUE_NAME = "flowforge.orders"
ORDER_CREATED_ROUTING_KEY = "order.created"

RETRY_EXCHANGE_NAME = "flowforge.retry"
RETRY_ROUTING_KEY = "order.retry"
RETRY_QUEUE_NAME = "flowforge.orders.retry"

DEAD_LETTER_EXCHANGE_NAME = "flowforge.dead-letter"
DEAD_LETTER_ROUTING_KEY = "order.dead"
DEAD_LETTER_QUEUE_NAME = "flowforge.orders.dlq"


def declare_topology(channel) -> None:
    """Declare durable primary, retry, and dead-letter routes idempotently."""
    channel.exchange_declare(
        exchange=EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )
    channel.queue_declare(queue=ORDERS_QUEUE_NAME, durable=True)
    channel.queue_bind(
        exchange=EXCHANGE_NAME,
        queue=ORDERS_QUEUE_NAME,
        routing_key=ORDER_CREATED_ROUTING_KEY,
    )

    channel.exchange_declare(
        exchange=RETRY_EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )
    channel.queue_declare(
        queue=RETRY_QUEUE_NAME,
        durable=True,
        arguments={
            "x-dead-letter-exchange": EXCHANGE_NAME,
            "x-dead-letter-routing-key": ORDER_CREATED_ROUTING_KEY,
        },
    )
    channel.queue_bind(
        exchange=RETRY_EXCHANGE_NAME,
        queue=RETRY_QUEUE_NAME,
        routing_key=RETRY_ROUTING_KEY,
    )

    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE_NAME,
        exchange_type=EXCHANGE_TYPE,
        durable=True,
    )
    channel.queue_declare(queue=DEAD_LETTER_QUEUE_NAME, durable=True)
    channel.queue_bind(
        exchange=DEAD_LETTER_EXCHANGE_NAME,
        queue=DEAD_LETTER_QUEUE_NAME,
        routing_key=DEAD_LETTER_ROUTING_KEY,
    )

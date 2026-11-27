"""Shared RabbitMQ topology for order integration events."""

EXCHANGE_NAME = "flowforge.events"
EXCHANGE_TYPE = "direct"
ORDERS_QUEUE_NAME = "flowforge.orders"
ORDER_CREATED_ROUTING_KEY = "order.created"


def declare_topology(channel) -> None:
    """Declare durable exchange, queue, and binding idempotently."""
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

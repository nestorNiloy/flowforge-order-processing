"""Consume OrderCreated events with transactional inbox deduplication."""

import json
import logging
import time
from decimal import Decimal, InvalidOperation

import pika
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.messaging.topology import ORDERS_QUEUE_NAME, declare_topology
from app.models.processed_message import OrderProcessingResult, ProcessedMessage

logger = logging.getLogger("flowforge.worker")


class InvalidEventError(ValueError):
    """Raised when a message does not match the supported event contract."""


def _validate_event(event: dict, metadata: dict) -> tuple[str, dict, str, str]:
    if not isinstance(event, dict):
        raise InvalidEventError("Event body must be a JSON object.")

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    schema_version = event.get("schema_version")
    order = event.get("order")

    if not isinstance(event_id, str) or not event_id:
        raise InvalidEventError("Event is missing a valid event_id.")
    if event_id != metadata.get("message_id"):
        raise InvalidEventError("RabbitMQ message_id does not match the event_id.")
    if event_type != "OrderCreated" or metadata.get("event_type") != event_type:
        raise InvalidEventError("Only OrderCreated events are supported.")
    if schema_version != 1 or metadata.get("schema_version") != 1:
        raise InvalidEventError("Unsupported OrderCreated schema version.")
    if not isinstance(order, dict):
        raise InvalidEventError("OrderCreated event is missing its order object.")

    order_id = order.get("id")
    customer_reference = order.get("customer_reference")
    aggregate_type = metadata.get("aggregate_type")
    aggregate_id = metadata.get("aggregate_id")

    if not isinstance(order_id, int) or order_id <= 0:
        raise InvalidEventError("Order ID must be a positive integer.")
    if not isinstance(customer_reference, str) or not customer_reference:
        raise InvalidEventError("Order customer_reference is required.")
    if aggregate_type != "order" or aggregate_id != str(order_id):
        raise InvalidEventError("RabbitMQ aggregate headers do not match the order.")

    try:
        total_amount = Decimal(str(order["total_amount"]))
    except (KeyError, InvalidOperation, TypeError, ValueError) as exc:
        raise InvalidEventError("Order total_amount must be a valid decimal.") from exc

    if not total_amount.is_finite() or total_amount < 0:
        raise InvalidEventError("Order total_amount must be a non-negative finite decimal.")

    return event_id, order, customer_reference, str(total_amount)


def process_order_created(
    session_factory,
    event: dict,
    metadata: dict,
) -> bool:
    """Persist inbox record and downstream result atomically.

    PostgreSQL ON CONFLICT DO NOTHING makes the event ID the concurrency-safe
    idempotency key. A duplicate returns False and creates no second result.
    """
    event_id, order, customer_reference, total_amount = _validate_event(event, metadata)

    with session_factory() as session:
        with session.begin():
            inserted_event_id = session.execute(
                pg_insert(ProcessedMessage)
                .values(
                    event_id=event_id,
                    event_type="OrderCreated",
                    schema_version=1,
                    aggregate_type="order",
                    aggregate_id=str(order["id"]),
                )
                .on_conflict_do_nothing(index_elements=[ProcessedMessage.event_id])
                .returning(ProcessedMessage.event_id)
            ).scalar_one_or_none()

            if inserted_event_id is None:
                logger.info("Ignoring duplicate OrderCreated event_id=%s", event_id)
                return False

            session.add(
                OrderProcessingResult(
                    event_id=event_id,
                    order_id=order["id"],
                    customer_reference=customer_reference,
                    total_amount=Decimal(total_amount),
                    processing_status="READY_FOR_FULFILLMENT",
                )
            )

    logger.info(
        "Processed OrderCreated event_id=%s order_id=%s",
        event_id,
        order["id"],
    )
    return True


def handle_delivery(channel, method, properties, body, *, session_factory=SessionLocal) -> None:
    """Ack only after the inbox and processing result transaction commits."""
    try:
        event = json.loads(body.decode("utf-8"))
        headers = properties.headers or {}
        metadata = {
            "message_id": properties.message_id,
            "event_type": properties.type,
            "schema_version": headers.get("schema_version"),
            "aggregate_type": headers.get("aggregate_type"),
            "aggregate_id": headers.get("aggregate_id"),
        }
        process_order_created(session_factory, event, metadata)
    except Exception:
        logger.exception("Order event processing failed; requeueing delivery")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    # process_order_created has exited its transaction before this acknowledgement.
    channel.basic_ack(delivery_tag=method.delivery_tag)


def run_forever() -> None:
    """Consume the durable order queue and reconnect after broker failures."""
    settings = get_settings()
    parameters = pika.URLParameters(settings.rabbitmq_url)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    parameters.socket_timeout = 5
    parameters.connection_attempts = 1

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("Starting FlowForge order worker")

    while True:
        try:
            with pika.BlockingConnection(parameters) as connection:
                channel = connection.channel()
                declare_topology(channel)
                channel.basic_qos(prefetch_count=1)
                channel.basic_consume(
                    queue=ORDERS_QUEUE_NAME,
                    on_message_callback=lambda ch, method, properties, body: handle_delivery(
                        ch, method, properties, body
                    ),
                    auto_ack=False,
                )
                logger.info("Worker consuming queue=%s", ORDERS_QUEUE_NAME)
                channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Worker shutdown requested")
            return
        except Exception:
            logger.exception(
                "Worker connection failed; reconnecting in %.1f seconds",
                settings.worker_reconnect_seconds,
            )
            time.sleep(settings.worker_reconnect_seconds)


if __name__ == "__main__":
    run_forever()

"""Continuously publish pending outbox events to RabbitMQ.

The database lease prevents multiple publisher instances from routinely
publishing the same row at the same time. Delivery remains at-least-once:
a process crash after broker confirmation but before the database update
can cause a later retry, which consumers must handle idempotently.
"""

import json
import logging
import time
from uuid import uuid4

import pika
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.messaging.topology import (
    EXCHANGE_NAME,
    ORDER_CREATED_ROUTING_KEY,
    declare_topology,
)

logger = logging.getLogger("flowforge.publisher")

CLAIM_PENDING_SQL = text(
    """
    WITH candidates AS (
        SELECT id
        FROM outbox_events
        WHERE status = 'PENDING'
          AND available_at <= CURRENT_TIMESTAMP
          AND (locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)
        ORDER BY created_at, id
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox_events AS event
    SET locked_until = CURRENT_TIMESTAMP + (:lease_seconds * INTERVAL '1 second'),
        locked_by = :worker_id,
        attempts = event.attempts + 1
    FROM candidates
    WHERE event.id = candidates.id
    RETURNING
        event.id,
        event.event_id,
        event.event_type,
        event.schema_version,
        event.aggregate_type,
        event.aggregate_id,
        event.payload
    """
)

MARK_PUBLISHED_SQL = text(
    """
    UPDATE outbox_events
    SET status = 'PUBLISHED',
        published_at = CURRENT_TIMESTAMP,
        locked_until = NULL,
        locked_by = NULL,
        last_error = NULL
    WHERE id = :id
      AND status = 'PENDING'
      AND locked_by = :worker_id
    """
)

MARK_FAILED_SQL = text(
    """
    UPDATE outbox_events
    SET available_at = CURRENT_TIMESTAMP + (:retry_delay_seconds * INTERVAL '1 second'),
        locked_until = NULL,
        locked_by = NULL,
        last_error = :last_error
    WHERE id = :id
      AND status = 'PENDING'
      AND locked_by = :worker_id
    """
)

RETRY_DELAY_SECONDS = 5


def claim_pending_events(
    session_factory,
    *,
    worker_id: str,
    batch_size: int,
    lease_seconds: int,
) -> list[dict]:
    """Atomically lease eligible rows and commit before any broker I/O."""
    with session_factory() as session:
        with session.begin():
            rows = session.execute(
                CLAIM_PENDING_SQL,
                {
                    "worker_id": worker_id,
                    "batch_size": batch_size,
                    "lease_seconds": lease_seconds,
                },
            ).mappings().all()
            return [dict(row) for row in rows]


def mark_published(session_factory, *, event_id: int, worker_id: str) -> None:
    """Mark a leased event published, but only after broker confirmation."""
    with session_factory() as session:
        with session.begin():
            result = session.execute(
                MARK_PUBLISHED_SQL,
                {"id": event_id, "worker_id": worker_id},
            )
            if result.rowcount != 1:
                raise RuntimeError(
                    f"Outbox lease was lost before event row {event_id} could be marked published"
                )


def mark_failed(
    session_factory,
    *,
    event_id: int,
    worker_id: str,
    error: Exception,
) -> None:
    """Release a failed event for retry without changing its pending status."""
    with session_factory() as session:
        with session.begin():
            result = session.execute(
                MARK_FAILED_SQL,
                {
                    "id": event_id,
                    "worker_id": worker_id,
                    "retry_delay_seconds": RETRY_DELAY_SECONDS,
                    "last_error": str(error)[:1000],
                },
            )
            if result.rowcount != 1:
                logger.warning(
                    "Could not release outbox row %s because its lease is no longer owned by %s",
                    event_id,
                    worker_id,
                )


def publish_event(channel, event: dict) -> None:
    """Publish one persistent event and require a positive broker confirmation."""
    body = json.dumps(
        event["payload"],
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")

    confirmed = channel.basic_publish(
        exchange=EXCHANGE_NAME,
        routing_key=ORDER_CREATED_ROUTING_KEY,
        body=body,
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=2,
            message_id=event["event_id"],
            type=event["event_type"],
            headers={
                "schema_version": event["schema_version"],
                "aggregate_type": event["aggregate_type"],
                "aggregate_id": event["aggregate_id"],
            },
        ),
        mandatory=True,
    )
    if confirmed is False:
        raise RuntimeError(f"RabbitMQ did not confirm event {event['event_id']}")


def publish_batch(
    channel,
    *,
    session_factory=SessionLocal,
    worker_id: str,
    batch_size: int | None = None,
    lease_seconds: int | None = None,
) -> int:
    """Publish one claimed batch; return the number successfully confirmed."""
    settings = get_settings()
    events = claim_pending_events(
        session_factory,
        worker_id=worker_id,
        batch_size=batch_size or settings.outbox_batch_size,
        lease_seconds=lease_seconds or settings.outbox_lease_seconds,
    )
    published = 0

    for event in events:
        try:
            publish_event(channel, event)
            mark_published(
                session_factory,
                event_id=event["id"],
                worker_id=worker_id,
            )
            published += 1
            logger.info(
                "Published %s event_id=%s aggregate_id=%s",
                event["event_type"],
                event["event_id"],
                event["aggregate_id"],
            )
        except Exception as exc:
            try:
                mark_failed(
                    session_factory,
                    event_id=event["id"],
                    worker_id=worker_id,
                    error=exc,
                )
            except Exception:
                logger.exception("Could not release failed outbox row %s", event["id"])
            raise

    return published


def run_forever() -> None:
    """Reconnect to RabbitMQ indefinitely; pending rows remain durable in PostgreSQL."""
    settings = get_settings()
    worker_id = str(uuid4())

    parameters = pika.URLParameters(settings.rabbitmq_url)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    parameters.socket_timeout = 5
    parameters.connection_attempts = 1

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info("Starting outbox publisher worker_id=%s", worker_id)

    while True:
        try:
            with pika.BlockingConnection(parameters) as connection:
                channel = connection.channel()
                declare_topology(channel)
                channel.confirm_delivery()
                logger.info("Connected to RabbitMQ; publisher confirms enabled")

                while connection.is_open and channel.is_open:
                    count = publish_batch(channel, worker_id=worker_id)
                    if count == 0:
                        time.sleep(settings.outbox_poll_interval_seconds)
        except KeyboardInterrupt:
            logger.info("Publisher shutdown requested")
            return
        except Exception:
            logger.exception(
                "Publisher cycle failed; reconnecting in %.1f seconds",
                settings.publisher_reconnect_seconds,
            )
            time.sleep(settings.publisher_reconnect_seconds)


if __name__ == "__main__":
    run_forever()

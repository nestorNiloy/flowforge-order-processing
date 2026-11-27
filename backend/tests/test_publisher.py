import os
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pika
import pytest
from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.messaging.publisher import (
    claim_pending_events,
    publish_batch,
)
from app.messaging.topology import (
    EXCHANGE_NAME,
    ORDER_CREATED_ROUTING_KEY,
    ORDERS_QUEUE_NAME,
    declare_topology,
)
from app.models import OutboxEvent


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")


class FakeChannel:
    def __init__(self, confirmation=True):
        self.confirmation = confirmation
        self.messages = []

    def basic_publish(self, **kwargs):
        self.messages.append(kwargs)
        return self.confirmation


@pytest.fixture()
def publisher_session_factory():
    if not TEST_DATABASE_URL or not TEST_DATABASE_URL.startswith("postgresql"):
        pytest.skip("Outbox publisher claiming requires PostgreSQL row locks.")

    engine = create_engine(TEST_DATABASE_URL, pool_pre_ping=True)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


def add_pending_event(session_factory):
    event_id = str(uuid4())
    now = datetime.now(timezone.utc)
    with session_factory() as session:
        session.add(
            OutboxEvent(
                event_id=event_id,
                event_type="OrderCreated",
                schema_version=1,
                aggregate_type="order",
                aggregate_id="9001",
                payload={
                    "event_id": event_id,
                    "event_type": "OrderCreated",
                    "schema_version": 1,
                    "occurred_at": now.isoformat(),
                    "order": {
                        "id": 9001,
                        "customer_reference": "publisher-test",
                        "status": "CONFIRMED",
                        "total_amount": "12.50",
                        "items": [],
                    },
                },
                status="PENDING",
                occurred_at=now,
            )
        )
        session.commit()
    return event_id


def get_event(session_factory, event_id):
    with session_factory() as session:
        return session.scalar(
            select(OutboxEvent).where(OutboxEvent.event_id == event_id)
        )


def test_confirmed_publish_marks_event_published(publisher_session_factory):
    event_id = add_pending_event(publisher_session_factory)
    channel = FakeChannel(confirmation=True)

    published = publish_batch(
        channel,
        session_factory=publisher_session_factory,
        worker_id="publisher-test",
    )

    assert published == 1
    assert len(channel.messages) == 1
    message = channel.messages[0]
    assert message["exchange"] == EXCHANGE_NAME
    assert message["routing_key"] == ORDER_CREATED_ROUTING_KEY
    assert message["mandatory"] is True
    assert message["properties"].delivery_mode == 2
    assert message["properties"].message_id == event_id

    stored = get_event(publisher_session_factory, event_id)
    assert stored.status == "PUBLISHED"
    assert stored.attempts == 1
    assert stored.published_at is not None
    assert stored.locked_by is None
    assert stored.locked_until is None
    assert stored.last_error is None


def test_negative_broker_confirmation_keeps_event_pending(publisher_session_factory):
    event_id = add_pending_event(publisher_session_factory)
    channel = FakeChannel(confirmation=False)

    with pytest.raises(RuntimeError, match="did not confirm"):
        publish_batch(
            channel,
            session_factory=publisher_session_factory,
            worker_id="publisher-test",
        )

    stored = get_event(publisher_session_factory, event_id)
    assert stored.status == "PENDING"
    assert stored.attempts == 1
    assert stored.published_at is None
    assert stored.locked_by is None
    assert stored.locked_until is None
    assert stored.last_error
    assert stored.available_at > datetime.now(timezone.utc)


def test_lease_prevents_parallel_claim_and_recovers_after_publisher_crash(
    publisher_session_factory,
):
    event_id = add_pending_event(publisher_session_factory)

    first_claim = claim_pending_events(
        publisher_session_factory,
        worker_id="publisher-one",
        batch_size=10,
        lease_seconds=60,
    )
    assert [row["event_id"] for row in first_claim] == [event_id]

    second_claim = claim_pending_events(
        publisher_session_factory,
        worker_id="publisher-two",
        batch_size=10,
        lease_seconds=60,
    )
    assert second_claim == []

    # Simulate the first publisher crashing: its lease expires without publishing.
    with publisher_session_factory() as session:
        session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.event_id == event_id)
            .values(locked_until=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        session.commit()

    recovered_claim = claim_pending_events(
        publisher_session_factory,
        worker_id="publisher-two",
        batch_size=10,
        lease_seconds=60,
    )
    assert [row["event_id"] for row in recovered_claim] == [event_id]
    assert recovered_claim[0]["event_id"] == event_id
    assert get_event(publisher_session_factory, event_id).attempts == 2


@pytest.mark.skipif(not RABBITMQ_URL, reason="Set RABBITMQ_URL to run RabbitMQ integration test.")
def test_rabbitmq_accepts_and_delivers_persistent_order_event():
    parameters = pika.URLParameters(RABBITMQ_URL)
    parameters.connection_attempts = 1
    parameters.socket_timeout = 5

    with pika.BlockingConnection(parameters) as connection:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()

        event_id = str(uuid4())
        body = b'{"event_type":"OrderCreated","schema_version":1}'
        confirmed = channel.basic_publish(
            exchange=EXCHANGE_NAME,
            routing_key=ORDER_CREATED_ROUTING_KEY,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,
                message_id=event_id,
                type="OrderCreated",
            ),
            mandatory=True,
        )
        assert confirmed is not False

        method, properties, received_body = channel.basic_get(
            queue=ORDERS_QUEUE_NAME,
            auto_ack=False,
        )
        assert method is not None
        assert properties.message_id == event_id
        assert properties.delivery_mode == 2
        assert received_body == body
        channel.basic_ack(delivery_tag=method.delivery_tag)

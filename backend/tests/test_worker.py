from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pika
import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from app.db.session import Base
from app.messaging.worker import handle_delivery, process_order_created
from app.models import Order, OrderProcessingResult, ProcessedMessage


class FakeChannel:
    def __init__(self):
        self.acknowledged = []
        self.rejected = []

    def basic_ack(self, *, delivery_tag):
        self.acknowledged.append(delivery_tag)

    def basic_nack(self, *, delivery_tag, requeue):
        self.rejected.append((delivery_tag, requeue))


@pytest.fixture()
def worker_session_factory():
    import os

    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url or not database_url.startswith("postgresql"):
        pytest.skip("The idempotent worker tests require PostgreSQL.")

    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    try:
        yield factory
    finally:
        Base.metadata.drop_all(bind=engine)
        engine.dispose()


def create_order(session_factory) -> int:
    with session_factory() as session:
        order = Order(
            customer_reference="worker-test",
            status="CONFIRMED",
            total_amount=Decimal("12.50"),
        )
        session.add(order)
        session.commit()
        return order.id


def make_delivery(order_id: int, *, event_id: str | None = None):
    event_id = event_id or str(uuid4())
    event = {
        "event_id": event_id,
        "event_type": "OrderCreated",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "order": {
            "id": order_id,
            "customer_reference": "worker-test",
            "status": "CONFIRMED",
            "total_amount": "12.50",
            "items": [],
        },
    }
    properties = pika.BasicProperties(
        content_type="application/json",
        delivery_mode=2,
        message_id=event_id,
        type="OrderCreated",
        headers={
            "schema_version": 1,
            "aggregate_type": "order",
            "aggregate_id": str(order_id),
        },
    )
    import json

    return event, properties, json.dumps(event).encode("utf-8")


def count_rows(session_factory, model) -> int:
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(model))


def test_worker_persists_inbox_and_result_once_for_duplicate_event(worker_session_factory):
    order_id = create_order(worker_session_factory)
    event, properties, body = make_delivery(order_id)
    metadata = {
        "message_id": properties.message_id,
        "event_type": properties.type,
        "schema_version": properties.headers["schema_version"],
        "aggregate_type": properties.headers["aggregate_type"],
        "aggregate_id": properties.headers["aggregate_id"],
    }

    first_result = process_order_created(worker_session_factory, event, metadata)
    duplicate_result = process_order_created(worker_session_factory, event, metadata)

    assert first_result is True
    assert duplicate_result is False
    assert count_rows(worker_session_factory, ProcessedMessage) == 1
    assert count_rows(worker_session_factory, OrderProcessingResult) == 1

    with worker_session_factory() as session:
        result = session.scalar(select(OrderProcessingResult))
        assert result.event_id == event["event_id"]
        assert result.order_id == order_id
        assert result.customer_reference == "worker-test"
        assert result.total_amount == Decimal("12.50")
        assert result.processing_status == "READY_FOR_FULFILLMENT"


def test_worker_acknowledges_after_successful_processing(worker_session_factory):
    order_id = create_order(worker_session_factory)
    _, properties, body = make_delivery(order_id)
    channel = FakeChannel()
    method = type("Method", (), {"delivery_tag": 41})()

    handle_delivery(
        channel,
        method,
        properties,
        body,
        session_factory=worker_session_factory,
    )

    assert channel.acknowledged == [41]
    assert channel.rejected == []
    assert count_rows(worker_session_factory, ProcessedMessage) == 1
    assert count_rows(worker_session_factory, OrderProcessingResult) == 1


def test_duplicate_delivery_is_acknowledged_without_duplicate_effect(worker_session_factory):
    order_id = create_order(worker_session_factory)
    _, properties, body = make_delivery(order_id)
    channel = FakeChannel()

    for delivery_tag in (51, 52):
        method = type("Method", (), {"delivery_tag": delivery_tag})()
        handle_delivery(
            channel,
            method,
            properties,
            body,
            session_factory=worker_session_factory,
        )

    assert channel.acknowledged == [51, 52]
    assert channel.rejected == []
    assert count_rows(worker_session_factory, ProcessedMessage) == 1
    assert count_rows(worker_session_factory, OrderProcessingResult) == 1


def test_failed_processing_rolls_back_inbox_and_requeues(worker_session_factory):
    # The missing order causes the downstream result's foreign key to fail.
    _, properties, body = make_delivery(999999)
    channel = FakeChannel()
    method = type("Method", (), {"delivery_tag": 61})()

    handle_delivery(
        channel,
        method,
        properties,
        body,
        session_factory=worker_session_factory,
    )

    assert channel.acknowledged == []
    assert channel.rejected == [(61, True)]
    assert count_rows(worker_session_factory, ProcessedMessage) == 0
    assert count_rows(worker_session_factory, OrderProcessingResult) == 0


def test_invalid_event_metadata_is_requeued(worker_session_factory):
    order_id = create_order(worker_session_factory)
    _, properties, body = make_delivery(order_id)
    properties.headers["schema_version"] = 99
    channel = FakeChannel()
    method = type("Method", (), {"delivery_tag": 71})()

    handle_delivery(
        channel,
        method,
        properties,
        body,
        session_factory=worker_session_factory,
    )

    assert channel.acknowledged == []
    assert channel.rejected == [(71, True)]
    assert count_rows(worker_session_factory, ProcessedMessage) == 0
    assert count_rows(worker_session_factory, OrderProcessingResult) == 0

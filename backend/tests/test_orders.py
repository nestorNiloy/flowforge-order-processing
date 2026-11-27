import os

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.session import Base, get_db
from app.main import app
from app.models import Order, OrderItem, OutboxEvent, Product


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
if TEST_DATABASE_URL:
    test_engine = create_engine(TEST_DATABASE_URL, pool_pre_ping=True)
else:
    test_engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

TestingSessionLocal = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture()
def db_session():
    Base.metadata.drop_all(bind=test_engine)
    Base.metadata.create_all(bind=test_engine)
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=test_engine)


@pytest.fixture()
def client(db_session):
    def override_get_db():
        request_session = TestingSessionLocal()
        try:
            yield request_session
        finally:
            request_session.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def add_product(db_session, *, sku="SKU-001", name="Mechanical Keyboard", price="79.90", quantity=10):
    product = Product(
        sku=sku,
        name=name,
        unit_price=price,
        available_quantity=quantity,
    )
    db_session.add(product)
    db_session.commit()
    db_session.refresh(product)
    return product


def order_payload(product_id, quantity=2):
    return {
        "customer_reference": "demo-customer-001",
        "items": [{"product_id": product_id, "quantity": quantity}],
    }


def test_create_order_reserves_stock_and_returns_price_snapshot(client, db_session):
    product = add_product(db_session)

    response = client.post("/api/v1/orders", json=order_payload(product.id, quantity=2))

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "CONFIRMED"
    assert body["total_amount"] == "159.80"
    assert body["items"][0]["product_name_snapshot"] == "Mechanical Keyboard"
    assert body["items"][0]["unit_price_snapshot"] == "79.90"
    assert body["items"][0]["quantity"] == 2

    db_session.refresh(product)
    assert product.available_quantity == 8

    events = db_session.query(OutboxEvent).all()
    assert len(events) == 1
    event = events[0]
    assert event.event_type == "OrderCreated"
    assert event.schema_version == 1
    assert event.aggregate_type == "order"
    assert event.aggregate_id == str(body["id"])
    assert event.status == "PENDING"
    assert event.payload["event_id"] == event.event_id
    assert event.payload["order"]["id"] == body["id"]
    assert event.payload["order"]["total_amount"] == "159.80"
    assert event.payload["order"]["items"][0]["product_name"] == "Mechanical Keyboard"


def test_order_item_keeps_snapshot_when_product_changes(client, db_session):
    product = add_product(db_session)
    created = client.post("/api/v1/orders", json=order_payload(product.id)).json()

    product.name = "Updated Keyboard"
    product.unit_price = "99.90"
    db_session.commit()

    response = client.get(f"/api/v1/orders/{created['id']}")

    assert response.status_code == 200
    assert response.json()["items"][0]["product_name_snapshot"] == "Mechanical Keyboard"
    assert response.json()["items"][0]["unit_price_snapshot"] == "79.90"


def test_unknown_product_returns_404(client):
    response = client.post("/api/v1/orders", json=order_payload(9999))

    assert response.status_code == 404
    assert response.json()["detail"]["product_id"] == 9999


def test_insufficient_stock_returns_409_without_creating_order(client, db_session):
    product = add_product(db_session, quantity=1)

    response = client.post("/api/v1/orders", json=order_payload(product.id, quantity=2))

    assert response.status_code == 409
    db_session.refresh(product)
    assert product.available_quantity == 1
    assert db_session.query(Order).count() == 0


def test_failed_multi_item_order_rolls_back_earlier_reservation(client, db_session):
    first = add_product(db_session, sku="SKU-001", quantity=5)
    second = add_product(db_session, sku="SKU-002", name="Mouse", price="25.00", quantity=1)
    payload = {
        "customer_reference": "demo-customer-001",
        "items": [
            {"product_id": first.id, "quantity": 3},
            {"product_id": second.id, "quantity": 2},
        ],
    }

    response = client.post("/api/v1/orders", json=payload)

    assert response.status_code == 409
    db_session.refresh(first)
    db_session.refresh(second)
    assert first.available_quantity == 5
    assert second.available_quantity == 1
    assert db_session.query(Order).count() == 0
    assert db_session.query(OrderItem).count() == 0
    assert db_session.query(OutboxEvent).count() == 0


def test_duplicate_product_lines_are_rejected(client):
    response = client.post(
        "/api/v1/orders",
        json={
            "customer_reference": "demo-customer-001",
            "items": [
                {"product_id": 1, "quantity": 1},
                {"product_id": 1, "quantity": 2},
            ],
        },
    )

    assert response.status_code == 422


def test_missing_order_returns_404(client):
    response = client.get("/api/v1/orders/123456")

    assert response.status_code == 404


@pytest.mark.skipif(
    not TEST_DATABASE_URL or not TEST_DATABASE_URL.startswith("postgresql"),
    reason="Concurrency behavior must be verified against PostgreSQL.",
)
def test_concurrent_orders_cannot_oversell_inventory(client, db_session):
    from concurrent.futures import ThreadPoolExecutor

    product = add_product(db_session, quantity=5)
    payload = order_payload(product.id, quantity=4)

    def submit():
        with TestClient(app) as concurrent_client:
            return concurrent_client.post("/api/v1/orders", json=payload).status_code

    with ThreadPoolExecutor(max_workers=2) as executor:
        statuses = list(executor.map(lambda _index: submit(), range(2)))

    assert sorted(statuses) == [201, 409]
    db_session.refresh(product)
    assert product.available_quantity == 1
    assert db_session.query(Order).count() == 1


def test_successful_order_creates_exactly_one_pending_outbox_event(client, db_session):
    product = add_product(db_session)

    response = client.post("/api/v1/orders", json=order_payload(product.id, quantity=3))

    assert response.status_code == 201
    event = db_session.query(OutboxEvent).one()
    assert event.status == "PENDING"
    assert event.attempts == 0
    assert event.event_id
    assert event.payload["order"]["items"][0]["quantity"] == 3


def test_failed_order_does_not_create_outbox_event(client, db_session):
    product = add_product(db_session, quantity=1)

    response = client.post("/api/v1/orders", json=order_payload(product.id, quantity=2))

    assert response.status_code == 409
    assert db_session.query(Order).count() == 0
    assert db_session.query(OutboxEvent).count() == 0

# FlowForge — Event-Driven Order Processing Platform

FlowForge is a backend engineering project demonstrating reliable order processing, inventory consistency, and asynchronous event-driven workflows. It is built as a learning and portfolio system; it does not process real payments or ship real orders.

## Project status

**Phase 1 — Foundation: complete**

FastAPI, PostgreSQL, SQLAlchemy, Alembic, Docker Compose and backend CI are in place.

**Phase 2 — Order domain: complete**

The API supports order creation and retrieval. Order creation reserves inventory in the same database transaction, using a conditional SQL update to prevent overselling. Order items preserve product-name and unit-price snapshots. The PostgreSQL test suite verifies rollback and concurrent inventory reservation.

**Phase 3 — Event-driven processing: in progress**

- **3.0 — Architecture and branch setup:** complete.
- **3.1 — Transactional outbox:** complete. Each successful order and its versioned `OrderCreated` event commit atomically.
- **3.2 — RabbitMQ and publisher:** implementation in progress. A separate publisher leases pending outbox rows, sends persistent messages to a durable RabbitMQ exchange/queue, and marks rows published only after broker confirmation.

The remaining increments add an idempotent worker, bounded retry/dead-letter handling, and end-to-end verification. See [docs/phase-3-architecture.md](docs/phase-3-architecture.md).

## Current architecture

- **FastAPI** validates requests and exposes the order API.
- **PostgreSQL** stores products, orders, order items, inventory and durable outbox events.
- **Outbox publisher** independently claims and publishes pending integration events.
- **RabbitMQ** provides a durable exchange and order queue.
- **SQLAlchemy** manages persistence and atomic transactions.
- **Alembic** versions database schema changes.
- **Docker Compose** runs the API, database, broker and publisher locally.
- **GitHub Actions** runs automated tests against PostgreSQL and RabbitMQ services.

The API does not depend on RabbitMQ availability. If the broker is down, order creation can continue and events remain pending in PostgreSQL.

## Run locally

Prerequisites: Docker Desktop with Docker Compose, Git and Python 3.12+.

~~~powershell
git clone https://github.com/nestorNiloy/flowforge-order-processing.git
cd flowforge-order-processing
git switch feature/phase-3-event-driven-foundation
docker compose up --build -d
~~~

The API is available at http://localhost:8000; interactive documentation is at http://localhost:8000/docs. RabbitMQ's management UI is at http://localhost:15672 (development login: `flowforge` / `flowforge_dev`).

Check API/database connectivity at http://localhost:8000/health.

## Database migrations

From the `backend/` directory, with the virtual environment activated:

~~~powershell
$env:DATABASE_URL = "postgresql+psycopg://flowforge:flowforge_dev@localhost:15432/flowforge"
alembic upgrade head
~~~

The latest migration adds publisher lease fields to the outbox table. The API and publisher containers connect to PostgreSQL internally through `db:5432`; the host uses port `15432`.

## API

### Create an order

POST /api/v1/orders

~~~json
{
  "customer_reference": "demo-customer-001",
  "items": [
    {"product_id": 1, "quantity": 2}
  ]
}
~~~

A successful request returns 201 Created, an order ID, its confirmed status, total amount and immutable item snapshots. The order, inventory reservation and `OrderCreated` outbox event are committed in one PostgreSQL transaction.

- 404: a requested product does not exist.
- 409: a requested quantity is not available.
- 422: request validation fails, including duplicate product IDs or non-positive quantities.

### Retrieve an order

GET /api/v1/orders/{order_id}

Returns the order and its stored item snapshots, or 404 when it does not exist.

## Run tests

From the repository root:

~~~powershell
$env:PYTHONPATH = "backend"
$env:TEST_DATABASE_URL = "postgresql+psycopg://flowforge:flowforge_dev@localhost:15432/flowforge_test"
$env:RABBITMQ_URL = "amqp://flowforge:flowforge_dev@localhost:5672/%2F"
python -m pytest backend/tests -v
~~~

The publisher lease tests require PostgreSQL because they verify row-locking and `SKIP LOCKED` behavior. The RabbitMQ integration test runs when `RABBITMQ_URL` is set. GitHub Actions starts temporary PostgreSQL and RabbitMQ services for the full suite.

The test suite creates and drops its tables in `flowforge_test`; never point `TEST_DATABASE_URL` at a database containing data you need to keep.

## Publisher behavior

- Pending events are claimed in short database transactions using `FOR UPDATE SKIP LOCKED` and a time-limited lease.
- Broker I/O happens after the claim transaction commits; database locks are not held while waiting on RabbitMQ.
- Messages are persistent and routed through a durable exchange to a durable queue.
- Publisher confirms and mandatory routing are enabled.
- An event is marked `PUBLISHED` only after a positive broker confirmation.
- Failed sends return to the pending state with a short retry delay. The bounded retry/dead-letter policy is implemented in Phase 3.4.
- A crash after broker confirmation but before the database update can cause redelivery. This is intentional at-least-once delivery; the worker added in Phase 3.3 will deduplicate by event ID.

## Engineering principles

- Reserve inventory and persist the order atomically.
- Use conditional database updates rather than unsafe read-then-write stock checks.
- Preserve historical product details on order items.
- Keep schema changes versioned and reviewable.
- Publish integration events through a transactional outbox rather than dual-writing to the database and broker.
- Design consumers for at-least-once delivery and idempotent processing.
- Test success paths, validation failures, rollback behavior, concurrency and failure recovery.

## Scope

This is a simulated order-processing backend. It does not process real payments, store real customer accounts or connect to a shipping provider.

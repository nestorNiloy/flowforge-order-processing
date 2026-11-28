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
- **3.2 — RabbitMQ and publisher:** implementation and local end-to-end publishing verification complete. The publisher leases pending outbox rows, sends persistent messages to a durable RabbitMQ exchange/queue, and marks rows published only after broker confirmation.
- **3.3 — Idempotent worker:** implementation, local end-to-end processing, 22-test suite and GitHub Actions CI verified.
- **3.4 — Retry and dead-letter handling:** implementation in progress; bounded retry handoff and durable retry/DLQ topology are being tested.
- **3.5 — End-to-end verification:** planned.

See [docs/phase-3-architecture.md](docs/phase-3-architecture.md) for design and failure semantics.

## Current architecture

- **FastAPI** validates requests and exposes the order API.
- **PostgreSQL** stores products, orders, order items, inventory, outbox events, processed-message inbox records and downstream processing results.
- **Outbox publisher** independently claims and publishes pending integration events.
- **RabbitMQ** provides durable primary, delayed-retry and dead-letter routes.
- **Order worker** consumes messages with manual acknowledgements, event-ID deduplication and bounded retry handling.
- **SQLAlchemy** manages persistence and atomic transactions.
- **Alembic** versions database schema changes.
- **Docker Compose** runs the API, database, broker, publisher and worker locally.
- **GitHub Actions** runs automated tests against PostgreSQL and RabbitMQ services.

The API does not depend on RabbitMQ availability. If the broker is down, order creation can continue and events remain pending in PostgreSQL.

## Run locally

Prerequisites: Docker Desktop with Docker Compose, Git and Python 3.12+.

~~~powershell
git clone https://github.com/nestorNiloy/flowforge-order-processing.git
cd flowforge-order-processing
git switch feature/phase-3-retry-dlq
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

Migration `0005_create_worker_tables` adds the `processed_messages` inbox and `order_processing_results` downstream result table. Phase 3.4 adds no database migration. The API, publisher and worker containers connect to PostgreSQL internally through `db:5432`; the host uses port `15432`.

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

The database tests require PostgreSQL because they verify transaction and row-locking behavior. RabbitMQ integration tests run when `RABBITMQ_URL` is set. GitHub Actions starts temporary PostgreSQL and RabbitMQ services for the full suite.

The test suite creates and drops its tables in `flowforge_test`; never point `TEST_DATABASE_URL` at a database containing data you need to keep.

## Publisher behavior

- Pending events are claimed in short database transactions using `FOR UPDATE SKIP LOCKED` and a time-limited lease.
- Broker I/O happens after the claim transaction commits; database locks are not held while waiting on RabbitMQ.
- Messages are persistent and routed through a durable exchange to a durable queue.
- Publisher confirms and mandatory routing are enabled.
- An event is marked `PUBLISHED` only after a positive broker confirmation.
- Failed sends return to the pending state with a short retry delay.
- A crash after broker confirmation but before the database update can cause redelivery. This is intentional at-least-once delivery.

## Worker behavior

- The worker consumes `OrderCreated` messages from the durable `flowforge.orders` queue with manual acknowledgements and a prefetch limit of one.
- It validates the event ID, event type, schema version and aggregate metadata before processing.
- A PostgreSQL inbox row with a unique event ID and a downstream `READY_FOR_FULFILLMENT` result are written in the same transaction.
- PostgreSQL `ON CONFLICT DO NOTHING` makes duplicate event delivery safe, including concurrent deliveries.
- The worker acknowledges only after the database transaction commits. Duplicate events are also acknowledged after confirming the prior processing record exists.
- Failed deliveries are republished to a durable retry queue with a per-message delay and an incremented retry-count header. The original delivery is acknowledged only after RabbitMQ confirms the retry handoff.
- The retry queue dead-letters expired messages back to the primary order exchange.
- After the configured retry limit (default: 3 retries), the worker publishes the message and failure reason to the durable `flowforge.orders.dlq` queue, then acknowledges the original delivery.
- If retry/DLQ publishing is not confirmed, the original delivery is negatively acknowledged with requeue enabled so it is not lost.
- The default retry delay is 5 seconds. Configure `WORKER_MAX_RETRIES` and `WORKER_RETRY_DELAY_MS` on the worker service to change the policy.

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

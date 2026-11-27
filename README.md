# FlowForge — Event-Driven Order Processing Platform

FlowForge is a backend engineering project demonstrating reliable order processing, inventory consistency, and asynchronous event-driven workflows. It is built as a learning and portfolio system; it does not process real payments or ship real orders.

## Project status

**Phase 1 — Foundation: complete**

FastAPI, PostgreSQL, SQLAlchemy, Alembic, Docker Compose and backend CI are in place.

**Phase 2 — Order domain: complete**

The API supports order creation and retrieval. Order creation reserves inventory in the same database transaction, using a conditional SQL update to prevent overselling. Order items preserve product-name and unit-price snapshots. The PostgreSQL test suite verifies rollback and concurrent inventory reservation.

**Phase 3 — Event-driven processing: in progress**

The architecture is documented in [docs/phase-3-architecture.md](docs/phase-3-architecture.md). The first implementation increment adds a PostgreSQL outbox table and records a versioned `OrderCreated` event in the same transaction as order creation and inventory reservation. RabbitMQ publishing and the independent worker are subsequent increments.

## Current architecture

- **FastAPI** validates requests and exposes the order API.
- **PostgreSQL** stores products, orders, order items, inventory and durable outbox events.
- **SQLAlchemy** manages persistence and atomic transactions.
- **Alembic** versions database schema changes.
- **Docker Compose** runs local infrastructure.
- **GitHub Actions** runs automated tests against PostgreSQL.

The target Phase 3 architecture adds an outbox publisher and an independent message consumer. See the [Phase 3 architecture and reliability plan](docs/phase-3-architecture.md).

## Run locally

Prerequisites: Docker Desktop with Docker Compose, Git and Python 3.12+.

```bash
git clone https://github.com/nestorNiloy/flowforge-order-processing.git
cd flowforge-order-processing
git switch main
docker compose up --build -d
```

The API is available at http://localhost:8000; interactive documentation is at http://localhost:8000/docs.

Check connectivity at http://localhost:8000/health.

## Database migrations

From the backend/ directory, with the virtual environment activated:

```powershell
$env:DATABASE_URL = "postgresql+psycopg://flowforge:flowforge_dev@localhost:15432/flowforge"
alembic upgrade head
```

The migration creates the order tables and database constraints. If your local PostgreSQL host port differs, adjust DATABASE_URL accordingly. The API container connects to PostgreSQL internally through db:5432.

## API

### Create an order

POST /api/v1/orders

```json
{
  "customer_reference": "demo-customer-001",
  "items": [
    {"product_id": 1, "quantity": 2}
  ]
}
```

A successful request returns 201 Created, an order ID, its confirmed status, total amount and immutable item snapshots.

- 404: a requested product does not exist.
- 409: a requested quantity is not available.
- 422: request validation fails, including duplicate product IDs or non-positive quantities.

### Retrieve an order

GET /api/v1/orders/{order_id}

Returns the order and its stored item snapshots, or 404 when it does not exist.

## Run tests

From the repository root:

```powershell
$env:PYTHONPATH = "backend"
python -m pytest backend/tests -v
```

By default, the order tests use an isolated in-memory SQLite database for fast local feedback. Set TEST_DATABASE_URL to a dedicated PostgreSQL test database to also run the PostgreSQL concurrency test. The test suite creates and drops its tables, so never point it at a database containing data you need to keep.

Example using a separate local test database:

```powershell
docker compose exec db psql -U flowforge -d flowforge -c "CREATE DATABASE flowforge_test;"
$env:TEST_DATABASE_URL = "postgresql+psycopg://flowforge:flowforge_dev@localhost:15432/flowforge_test"
python -m pytest backend/tests -v
```

GitHub Actions runs the complete test suite against a temporary PostgreSQL 16 service.

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

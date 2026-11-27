# FlowForge — Event-Driven Order Processing Platform

FlowForge is a backend engineering project that demonstrates reliable asynchronous order processing. It uses an online-shop scenario to explore event-driven communication, inventory consistency, retries, idempotency and failure recovery.

## Project status

**Phase 1 — Foundation (in progress)**

The repository currently contains a FastAPI application, PostgreSQL configuration, SQLAlchemy persistence, an initial Alembic migration, Docker Compose setup and backend tests.

## Planned architecture

- **FastAPI** receives and validates HTTP requests.
- **PostgreSQL** stores products, orders and processing state.
- **RabbitMQ** will deliver events between independent workers in a later phase.
- **Inventory worker** will reserve stock asynchronously.
- **Notification worker** will generate simulated order notifications.
- **React** will provide an operational dashboard after the backend is established.

## Prerequisites

- Docker Desktop with Docker Compose
- Git

## Run locally

1. Clone the repository and switch to the development branch:

   ```bash
   git clone https://github.com/nestorNiloy/flowforge-order-processing.git
   cd flowforge-order-processing
   git switch feature/phase-1-foundation
   ```

2. Start the API and PostgreSQL:

   ```bash
   docker compose up --build
   ```

3. Open the interactive API documentation at `http://localhost:8000/docs`.
4. Check application and database connectivity at `http://localhost:8000/health`.

Stop the services with `docker compose down`. Add `-v` only when you intentionally want to delete the local PostgreSQL data volume.

## Database migrations

From the `backend/` directory, install dependencies and apply the initial schema:

```bash
python -m pip install -r requirements.txt
alembic upgrade head
```

Create future schema revisions with:

```bash
alembic revision --autogenerate -m "describe schema change"
```

Review generated migration files before applying them. Autogeneration is a starting point, not a substitute for reviewing the schema diff.

## Run tests

From the repository root:

```bash
PYTHONPATH=backend python -m pytest backend/tests -v
```

The same test command runs in GitHub Actions for pushes and pull requests.

## Engineering principles

- Small, reviewable changes with meaningful commit messages.
- Explicit order-state transitions.
- Reliable event publication and idempotent event handling.
- Automated tests for normal and failure scenarios.
- Reproducible local development with Docker Compose.

## Scope

This is a simulated order-processing backend. It does not process real payments, handle real customer data or connect to a real shipping provider.
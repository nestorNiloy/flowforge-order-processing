# FlowForge — Event-Driven Order Processing Platform

FlowForge is a learning project that demonstrates reliable asynchronous order processing. It uses an online shop scenario to explore backend engineering concerns such as event-driven communication, inventory consistency, retries, idempotency and failure recovery.

## Project status

**Phase 1 — Foundation (in progress)**

The initial milestone establishes the FastAPI application, PostgreSQL persistence, database migrations, containerized local development and automated tests.

## Planned architecture

- **FastAPI** receives and validates HTTP requests.
- **PostgreSQL** stores orders, products and processing state.
- **RabbitMQ** will deliver events between independent workers in a later phase.
- **Inventory worker** will reserve stock asynchronously.
- **Notification worker** will generate simulated order notifications.
- **React** will provide an operational dashboard after the backend is established.

## Local development

Setup and run instructions will be added as the Phase 1 foundation is implemented.

## Engineering principles

- Small, reviewable changes with meaningful commit messages.
- Explicit order-state transitions.
- Reliable event publication and idempotent event handling.
- Automated tests for normal and failure scenarios.
- Reproducible local development with Docker Compose.

## Scope

This is a simulated order-processing backend. It does not process real payments, handle real customer data or connect to a real shipping provider.
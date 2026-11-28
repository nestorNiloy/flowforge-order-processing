# Phase 3 — Event-Driven Processing Architecture

## Goal

Evolve FlowForge from a synchronous order API into a small, reliable event-driven system while preserving the inventory guarantees established in Phase 2.

This is a learning and portfolio system. It will simulate downstream order processing; it will not handle real payments, customer accounts, or shipping.

## Architecture decision: transactional outbox

The API must not publish directly to RabbitMQ as part of the HTTP request. A database commit and a RabbitMQ publish cannot participate in one ordinary atomic transaction. Publishing before the database commit can expose an event for an order that later rolls back; publishing after commit can lose the event if the process crashes in between.

Instead, the API writes the order, reserves inventory, and inserts an outbox event in the **same PostgreSQL transaction**.

~~~text
                  synchronous request
Client ───────────────> FastAPI
                           |
                           | one PostgreSQL transaction
                           v
                    PostgreSQL
                    ├── orders
                    ├── order_items
                    ├── products (reserved stock)
                    └── outbox_events
                           |
                           | leased rows, publisher confirms
                           v
                 RabbitMQ: flowforge.events
                           |
                    flowforge.orders
                           |
                           v
                    independent worker
                      /          \
             success/duplicate   processing failure
                    |                 |
                    v                 v
             PostgreSQL       confirmed publish
             inbox + result    to retry exchange
                                     |
                              retry queue (TTL)
                                     |
                             dead-letter back to
                              primary exchange
                                     |
                               worker retries
                                     |
                          retry limit exhausted
                                     |
                             flowforge.orders.dlq
~~~ 

## Event contract

The first event is `OrderCreated`. It is an integration event, not a serialized SQLAlchemy model.

The versioned envelope contains:

- `event_id`: globally unique UUID, used for deduplication.
- `event_type`: `OrderCreated`.
- `schema_version`: initially `1`.
- `occurred_at`: UTC timestamp.
- `aggregate_type`: `order` (RabbitMQ message header).
- `aggregate_id`: order ID (RabbitMQ message header).
- `payload`: immutable order facts needed by the consumer (order ID, customer reference, total, and item snapshots).

The event payload uses the item name and unit-price snapshots already stored on the order. It must not depend on mutable current product data.

## Worker inbox and downstream effect

The worker treats the RabbitMQ message ID as the idempotency key.

1. It validates the JSON event envelope against the supported event type and schema version.
2. It verifies the RabbitMQ `message_id` matches the payload `event_id`, and that aggregate headers match the order ID.
3. In one PostgreSQL transaction, it inserts a `processed_messages` inbox row using `INSERT ... ON CONFLICT DO NOTHING`.
4. Only when that insert succeeds does it create the matching `order_processing_results` row with status `READY_FOR_FULFILLMENT`.
5. The inbox row and processing result commit or roll back together. The unique event ID prevents a duplicate delivery from repeating the downstream effect.
6. It acknowledges the RabbitMQ delivery only after the transaction has committed. A duplicate delivery is acknowledged after the existing inbox record is detected.

The processing result is an observable simulation of a downstream order workflow. It does not initiate real fulfilment or shipping.

## Bounded retry and dead-letter flow

The delivery guarantee remains **at-least-once with idempotent consumption**. A retry is a confirmed republish, not an unbounded broker requeue.

1. The worker channel enables publisher confirms as well as manual consumer acknowledgements.
2. If processing fails, the worker reads the `x-flowforge-retry-count` message header. A new message starts at zero.
3. While the count is below `WORKER_MAX_RETRIES` (default `3`), the worker republishes the original body to the durable retry exchange with the count incremented and a per-message expiration (default `5000` ms).
4. The durable retry queue holds the message until its expiration. Its dead-letter exchange and routing key return the message to the primary order exchange and queue.
5. Once the retry count reaches the configured limit, the worker publishes the original body to the durable dead-letter exchange/queue. It adds `x-flowforge-failure-reason` and retains the event ID and retry count for diagnosis.
6. The worker acknowledges the original delivery only after RabbitMQ confirms the retry or dead-letter publish. If the handoff cannot be confirmed, it negatively acknowledges the original with requeue enabled.
7. The retry queue and dead-letter queue are durable. Messages are persistent, and mandatory routing prevents silently accepting an unroutable handoff.

The retry limit counts retries after the first processing attempt. With the default limit of three, a message can be processed up to four times (the initial attempt plus three retries) before it is dead-lettered.

A crash after the retry/DLQ publish is confirmed but before the original delivery is acknowledged can produce a duplicate handoff. This is part of at-least-once delivery; stable event IDs and idempotent processing keep successful business effects safe. Failed attempts roll back their inbox/result transaction before retrying.

## Publisher and outbox failure semantics

1. The outbox row is committed atomically with the order.
2. A publisher claims pending rows safely so multiple publisher instances do not routinely publish the same row at the same time. A short PostgreSQL transaction uses `FOR UPDATE SKIP LOCKED` and assigns a time-limited lease; it commits before network I/O.
3. The publisher sends a persistent message to a durable RabbitMQ exchange/queue, uses mandatory routing, and waits for publisher confirmation.
4. Only after a positive broker confirmation does it mark the outbox row as published.
5. On a send failure, the publisher releases its lease, records the error, and makes the row eligible for another attempt after a short delay.
6. If the publisher crashes after RabbitMQ accepts a message but before the row is marked published, the lease eventually expires and the message may be sent again. This is expected.

The outbox publisher and consumer are independently runnable processes. The API depends on PostgreSQL, not RabbitMQ; if the broker is temporarily unavailable, events accumulate in PostgreSQL until publishing resumes.

## Planned implementation increments

### Phase 3.0 — Architecture and branch setup
- Create a dedicated feature branch from the merged Phase 2 `main`.
- Document the event contract, failure model, and acceptance criteria.
- No application behavior changes.
- **Status: complete.**

### Phase 3.1 — Outbox persistence
- Add an outbox SQLAlchemy model and Alembic migration.
- Add a versioned `OrderCreated` event schema/factory.
- Insert the outbox row inside the existing `create_order` transaction.
- Test that order, inventory reservation, and outbox row commit or roll back together.
- Do not require RabbitMQ yet.
- **Status: complete.**

### Phase 3.2 — RabbitMQ and publisher
- Add RabbitMQ to Docker Compose with a health check and persistent broker volume.
- Add a separate outbox publisher process.
- Declare a durable direct exchange and durable order queue.
- Publish persistent messages with mandatory routing and publisher confirms.
- Mark rows published only after confirmation; implement PostgreSQL `SKIP LOCKED` claiming and expiring leases.
- Keep broker I/O outside database transactions.
- Test positive/negative confirmations, concurrent claims, lease recovery after publisher crash, and real RabbitMQ delivery.
- **Status: implemented and locally verified end to end.**

### Phase 3.3 — Idempotent worker
- Add a separate worker process and durable queue consumer.
- Add a processed-message/inbox record with a unique event ID.
- Persist a downstream processing result in the same transaction as the inbox record.
- Validate event metadata and acknowledge only after commit.
- Test duplicate delivery, rollback, invalid events and acknowledgement behavior.
- **Status: implemented, locally verified end to end, 22 tests passing, and GitHub Actions CI passing.**

### Phase 3.4 — Retry and dead-letter handling
- Add durable retry and dead-letter exchanges/queues.
- Add configurable bounded retries and delayed redelivery.
- Confirm retry/DLQ handoff before acknowledging the original message.
- Preserve event identity and attach retry count/failure reason metadata.
- Test transient failure, exhausted retry, and failed handoff recovery.
- **Status: implementation in progress; local and CI verification pending.**

### Phase 3.5 — End-to-end verification
- Exercise API → PostgreSQL outbox → RabbitMQ → worker → PostgreSQL.
- Test API availability during broker outage, duplicate delivery, worker restart, and retry/DLQ behavior.
- Update README architecture, local run instructions, and operational notes.

## Acceptance criteria

- Existing Phase 2 API behavior and inventory concurrency guarantees remain intact.
- A successful order always has its corresponding outbox event in the same committed transaction.
- A failed order never leaves an outbox event or partial stock reservation.
- RabbitMQ downtime does not make order creation fail solely because the broker is unavailable.
- The publisher never marks an event published before broker confirmation.
- Concurrent publishers use leases so they do not routinely claim the same event.
- Expired leases allow recovery after a publisher crash.
- Duplicate deliveries do not duplicate the worker's business effect.
- Inbox record and downstream result commit or roll back atomically.
- Messages are acknowledged only after successful processing or a positively confirmed retry/DLQ handoff.
- Retry attempts are bounded; exhausted messages are inspectable in a durable dead-letter queue.
- A failed retry/DLQ handoff leaves the original message eligible for redelivery.
- CI tests database behavior and messaging integration without relying on external hosted services.

## Engineering constraints

- Keep the existing FastAPI API and PostgreSQL as the source of truth.
- Keep broker I/O outside the order-creation transaction and outside outbox claim transactions.
- Use explicit event schemas and schema versions; do not publish ORM objects.
- Use UTC timestamps and stable event IDs.
- Keep publisher and worker code separate from HTTP route handlers.
- Prefer deterministic tests and local Docker Compose services.
- Do not introduce payment, authentication, or shipping integrations in this phase.

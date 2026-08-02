---
name: kafka-outbox-relay
description: Build a scalable PostgreSQL outbox relay for Kafka with batching, retries, backpressure and consumer deduplication.
---

# Kafka Outbox Relay Skill

Relay loop:

1. Recover stale locks.
2. Claim bounded rows with `SKIP LOCKED`.
3. Publish using a long-lived producer.
4. Use batch APIs, compression and bounded in-flight operations.
5. Mark acknowledged rows published.
6. Back off failures with jitter.
7. Expose backlog age and publish metrics.

Use `acks=all` and producer idempotence where supported. Partition by the business key that requires ordering, usually account ID.

Semantics are at-least-once. A crash can occur after Kafka acknowledgement and before PostgreSQL status update. Consumers must deduplicate by `event_id`.

Never create one producer per request or per row.

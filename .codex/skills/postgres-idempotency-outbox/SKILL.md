---
name: postgres-idempotency-outbox
description: Implement concurrency-safe idempotency and a transactional PostgreSQL outbox for financial operations.
---

# PostgreSQL Idempotency and Outbox Skill

Required invariants:

1. Unique `idempotency_key` in PostgreSQL.
2. Deterministic canonical payload hash.
3. Same key and hash replays stored response.
4. Same key and different hash returns 409.
5. Transaction, journal, postings, balance updates and outbox commit together.
6. Kafka and Redis are never called inside the transaction.

Use integer minor units. Use parameterized SQL. Lock affected balances in deterministic account ID order. Add a low bounded retry for deadlocks/serialization failures.

Outbox claiming must use bounded batches and `FOR UPDATE SKIP LOCKED`. Publish after the claim transaction. Recover stale locks.

Tests:

- 100+ concurrent duplicate requests.
- Exactly one financial effect.
- One journal entry and expected postings.
- One outbox event.
- Atomic rollback.
- Multiple relay replicas.

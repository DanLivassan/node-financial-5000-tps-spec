---
name: financial-architecture
description: Design a high-throughput Node.js financial monorepo with atomic transaction processing, double-entry ledger, balances, reconciliation and event delivery.
---

# Financial Architecture Skill

Use PostgreSQL as the source of truth. A successful financial request must atomically create:

- Financial transaction.
- Journal entry.
- Balanced ledger postings.
- Balance projection updates.
- Outbox events.

Kafka, Redis and bank APIs are outside that transaction.

Review for:

- Short database transactions.
- Immutable ledger.
- Rebuildable balances.
- Separate relay and reconciliation workers.
- Bounded concurrency.
- Honest at-least-once semantics.
- Idempotency at request, consumer, import and reconciliation levels.

Reject:

- Direct PostgreSQL + Kafka dual writes.
- Direct PostgreSQL + Redis dual writes.
- Mutable ledger history.
- Balance-only accounting.
- Network calls inside DB transactions.
- End-to-end exactly-once claims.

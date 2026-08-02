---
name: testing-quality
description: Enforce unit, integration, property and failure-path testing for financial correctness and high-throughput behavior.
---

# Testing Quality Skill

Unit tests:

- Canonicalization and hash.
- Idempotency decisions.
- Double-entry validation.
- Posting generation.
- Balance updates.
- Insufficient funds.
- Lock ordering.
- Relay retry/backoff.
- Reconciliation matching and mismatches.
- Compensating entries.

Integration tests:

- Atomic transaction/journal/postings/balance/outbox commit.
- Full rollback.
- Concurrent duplicate requests.
- Hot-account contention.
- Balance rebuild.
- Multiple relays.
- Duplicate Kafka events.
- Idempotent statement imports.
- Safe reconciliation reruns.

Property tests:

- Debits always equal credits.
- Replays never alter balances.
- Reversal restores net balance.
- Independent transaction ordering does not alter final balances.

Do not mock PostgreSQL or Kafka for tests whose purpose is concurrency, locking or delivery behavior.

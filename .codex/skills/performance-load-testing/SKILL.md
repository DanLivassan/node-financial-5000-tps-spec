---
name: performance-load-testing
description: Create reproducible Autocannon and contention benchmarks for a Node.js financial API targeting 5,000 TPS.
---

# Performance Load Testing Skill

Required commands:

- `pnpm load:10s`
- `pnpm load:idempotency`
- `pnpm load:balance-contention`
- `pnpm load:report`
- `pnpm cleanup:transactions`

The 10-second Autocannon test must generate unique idempotency keys, save raw JSON and print p50/p95/p99, requests/sec, throughput and errors.

The idempotency test must send one key concurrently at least 100 times and query PostgreSQL to prove one transaction, one journal and no duplicated balance effect.

The balance contention test must compare distributed-account traffic with one hot account and report deadlocks, retries and final balance correctness.

Record environment, pool size, Kafka partitions, payload size, durability settings, CPU, memory and variance. Never present laptop Docker results as production capacity.

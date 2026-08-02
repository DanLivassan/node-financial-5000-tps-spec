---
name: ledger-reconciliation
description: Implement immutable double-entry accounting, balance projections and idempotent reconciliation against bank statements.
---

# Ledger and Reconciliation Skill

## Ledger rules

- Every journal entry has equal debit and credit totals.
- Ledger postings are immutable.
- Corrections use compensating entries.
- Money uses integer minor units.
- Balance projections are rebuildable from postings.
- Lock balance rows in deterministic order.
- Test hot-account contention.

## Bank statement import

- Deduplicate by provider, bank account and provider entry ID.
- Preserve raw payload.
- Store strong identifiers such as end-to-end ID.
- Import is safe to rerun.

## Matching priority

1. End-to-end ID.
2. Provider transaction ID.
3. External reference.
4. Amount, direction, currency and bounded time window as secondary evidence.

Never match solely by amount.

## Divergences

Detect:

- Missing at bank.
- Missing internally.
- Amount mismatch.
- Direction mismatch.
- Duplicate bank entry.
- Duplicate internal entry.
- Late settlement.
- Ambiguous/manual review.

## Period validation

Validate independently:

`opening + credits - debits = closing`

Compare:

- Ledger-derived balance.
- Projected internal balance.
- Bank-reported balance.

Reconciliation must be idempotent and must not block ingestion.

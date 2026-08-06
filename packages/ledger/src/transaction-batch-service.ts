import type pg from "pg";
import type { TransactionRequest } from "../../contracts/src/transactions.js";
import { metrics } from "../../observability/src/metrics.js";
import { loadAccounts } from "./account-repository.js";
import {
  validateAccounts, validateAndPrepareCommands, validateUniqueIdempotencyKeys, type ValidatedBatch,
} from "./batch-command-validator.js";
import { simulateCommands, type AcceptedCommand } from "./batch-simulator.js";
import { applyBalanceDeltas, createAndLockBalances } from "./balance-repository.js";
import { deterministicAccountOrder } from "./domain.js";
import { deleteRejectedTransactions, reserveFinancialTransactions } from "./financial-transaction-repository.js";
import { resolveExistingIdempotency, resolveReservationRaces } from "./idempotency-repository.js";
import { persistLedger } from "./ledger-repository.js";
import { persistOutbox } from "./outbox-repository.js";
import { DomainError, type TransactionResult } from "./transaction-service.js";
import { withTransactionRetry } from "./transaction-retry.js";

export interface BatchTransactionCommand {
  idempotencyKey: string;
  request: TransactionRequest;
  traceparent?: string;
}

export type BatchTransactionOutcome =
  | { ok: true; result: TransactionResult }
  | { ok: false; error: unknown };

export interface BatchExecutionHooks {
  beforeCommit?: () => void | Promise<void>;
}

function affectedAccountIds(items: Array<{ command: BatchTransactionCommand }>): string[] {
  return deterministicAccountOrder(items.flatMap((item) => [
    item.command.request.sourceAccountId, item.command.request.destinationAccountId,
  ]));
}

function resolveAcceptedOutcomes(
  outcomes: Array<BatchTransactionOutcome | undefined>, accepted: AcceptedCommand[],
): BatchTransactionOutcome[] {
  for (const item of accepted) {
    outcomes[item.index] = { ok: true, result: { statusCode: 201, body: item.response, replayed: false } };
  }
  for (const outcome of outcomes) if (!outcome) throw new Error("batch outcome was not resolved");
  return outcomes as BatchTransactionOutcome[];
}

function recordMetrics(outcomes: BatchTransactionOutcome[], createdCount: number): void {
  for (const outcome of outcomes) {
    if (outcome.ok && outcome.result.replayed) metrics.idempotencyReplays.inc();
    else if (!outcome.ok && outcome.error instanceof DomainError && outcome.error.code === "IDEMPOTENCY_CONFLICT") {
      metrics.idempotencyConflicts.inc();
    }
  }
  if (createdCount > 0) {
    metrics.transactionsCreated.inc(createdCount);
    metrics.ledgerEntries.inc(createdCount);
  }
}

async function executeBatchTransaction(
  client: pg.PoolClient, batch: ValidatedBatch, hooks: BatchExecutionHooks,
): Promise<{ outcomes: BatchTransactionOutcome[]; createdCount: number }> {
  const absent = await resolveExistingIdempotency(client, batch.prepared, batch.outcomes);
  const accounts = await loadAccounts(client, affectedAccountIds(absent));
  const valid = validateAccounts(absent, accounts, batch.outcomes);
  const reservedKeys = await reserveFinancialTransactions(client, valid);
  const reserved = await resolveReservationRaces(client, valid, reservedKeys, batch.outcomes);
  const lockedIds = affectedAccountIds(reserved);
  const balances = await createAndLockBalances(client, lockedIds);
  const simulation = simulateCommands(reserved, accounts, balances, batch.outcomes);
  await deleteRejectedTransactions(client, simulation.rejectedIds);
  await persistLedger(client, simulation.accepted);
  await applyBalanceDeltas(client, simulation.deltas);
  await persistOutbox(client, simulation.accepted);
  await hooks.beforeCommit?.();
  return {
    outcomes: resolveAcceptedOutcomes(batch.outcomes, simulation.accepted),
    createdCount: simulation.accepted.length,
  };
}

/** Posts independent financial commands in one durable PostgreSQL transaction. */
export async function createFinancialTransactionBatch(
  db: pg.Pool, commands: BatchTransactionCommand[], hooks: BatchExecutionHooks = {},
): Promise<BatchTransactionOutcome[]> {
  if (commands.length === 0) return [];
  validateUniqueIdempotencyKeys(commands);
  const batch = validateAndPrepareCommands(commands);
  const result = await withTransactionRetry(db, (client) => executeBatchTransaction(client, batch, hooks));
  recordMetrics(result.outcomes, result.createdCount);
  return result.outcomes;
}

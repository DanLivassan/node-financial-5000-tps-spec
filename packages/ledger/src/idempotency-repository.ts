import type pg from "pg";
import type { TransactionResponse } from "../../contracts/src/transactions.js";
import type { PreparedCommand } from "./batch-command-validator.js";
import { DomainError, type TransactionResult } from "./transaction-service.js";
import type { BatchTransactionOutcome } from "./transaction-batch-service.js";

interface PersistedTransaction {
  idempotency_key: string;
  request_hash: string;
  response_status: number;
  response_body: TransactionResponse;
}

function replay(row: PersistedTransaction): BatchTransactionOutcome {
  const result: TransactionResult = { statusCode: row.response_status, body: row.response_body, replayed: true };
  return { ok: true, result };
}

function conflict(): BatchTransactionOutcome {
  return { ok: false, error: new DomainError("IDEMPOTENCY_CONFLICT", 409,
    "Idempotency-Key was already used with another payload") };
}

async function findByKeys(client: pg.PoolClient, keys: string[]): Promise<Map<string, PersistedTransaction>> {
  if (keys.length === 0) return new Map();
  const result = await client.query<PersistedTransaction>(
    `SELECT idempotency_key,request_hash,response_status,response_body
     FROM financial_transactions WHERE idempotency_key=ANY($1::text[])`, [keys],
  );
  return new Map(result.rows.map((row) => [row.idempotency_key, row]));
}

export async function resolveExistingIdempotency(
  client: pg.PoolClient,
  prepared: PreparedCommand[],
  outcomes: Array<BatchTransactionOutcome | undefined>,
): Promise<PreparedCommand[]> {
  const existing = await findByKeys(client, prepared.map((item) => item.command.idempotencyKey));
  return prepared.filter((item) => {
    const row = existing.get(item.command.idempotencyKey);
    if (!row) return true;
    outcomes[item.index] = row.request_hash === item.hash ? replay(row) : conflict();
    return false;
  });
}

export async function resolveReservationRaces(
  client: pg.PoolClient,
  candidates: PreparedCommand[],
  insertedKeys: Set<string>,
  outcomes: Array<BatchTransactionOutcome | undefined>,
): Promise<PreparedCommand[]> {
  const lost = candidates.filter((item) => !insertedKeys.has(item.command.idempotencyKey));
  const winners = await findByKeys(client, lost.map((item) => item.command.idempotencyKey));
  for (const item of lost) {
    const winner = winners.get(item.command.idempotencyKey);
    if (!winner) throw new Error("idempotency winner disappeared");
    outcomes[item.index] = winner.request_hash === item.hash ? replay(winner) : conflict();
  }
  return candidates.filter((item) => insertedKeys.has(item.command.idempotencyKey));
}

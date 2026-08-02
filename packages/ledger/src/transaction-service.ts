import { randomUUID } from "node:crypto";
import type pg from "pg";
import type { TransactionRequest, TransactionResponse } from "../../contracts/src/transactions.js";
import { assertBalanced, deterministicAccountOrder, signedBalanceDelta, transferPostings, type AccountType } from "./domain.js";
import { requestHash } from "./idempotency.js";
import { metrics } from "../../observability/src/metrics.js";

export class DomainError extends Error {
  constructor(public readonly code: string, public readonly httpStatus: number, message: string) {
    super(message);
  }
}

interface AccountRow {
  id: string;
  account_type: AccountType;
  status: "active" | "blocked";
  allow_negative: boolean;
  currency: string;
  available_minor: bigint;
}

interface PersistedIdempotency {
  request_hash: string;
  response_status: number;
  response_body: TransactionResponse;
}

export interface TransactionResult {
  statusCode: number;
  body: TransactionResponse;
  replayed: boolean;
}

export async function createFinancialTransaction(
  db: pg.Pool,
  idempotencyKey: string,
  request: TransactionRequest,
  hooks: { beforeCommit?: () => void | Promise<void>; traceparent?: string } = {},
): Promise<TransactionResult> {
  if (!idempotencyKey || idempotencyKey.length > 128) {
    throw new DomainError("INVALID_IDEMPOTENCY_KEY", 400, "Idempotency-Key is required and must have at most 128 characters");
  }
  const hash = requestHash(request);
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      const transactionId = randomUUID();
      const journalEntryId = randomUUID();
      const createdAt = new Date().toISOString();
      const response: TransactionResponse = {
        id: transactionId,
        journalEntryId,
        externalReference: request.externalReference,
        status: "accepted",
        amountMinor: request.amountMinor,
        currency: request.currency,
        createdAt,
      };
      const inserted = await client.query(
        `INSERT INTO financial_transactions(
           id,idempotency_key,external_reference,source_account_id,destination_account_id,
           amount_minor,currency,status,request_hash,response_status,response_body,created_at,end_to_end_id,provider_transaction_id)
         VALUES ($1,$2,$3,$4,$5,$6,$7,'accepted',$8,201,$9,$10,$11,$12)
         ON CONFLICT (idempotency_key) DO NOTHING RETURNING id`,
        [transactionId, idempotencyKey, request.externalReference, request.sourceAccountId,
          request.destinationAccountId, request.amountMinor, request.currency, hash, response, createdAt,
          request.endToEndId ?? null, request.providerTransactionId ?? null],
      );
      if (inserted.rowCount === 0) {
        const existing = await client.query<PersistedIdempotency>(
          `SELECT request_hash,response_status,response_body
           FROM financial_transactions WHERE idempotency_key=$1`, [idempotencyKey],
        );
        const persisted = existing.rows[0];
        if (!persisted) throw new Error("idempotency winner disappeared");
        if (persisted.request_hash !== hash) {
          metrics.idempotencyConflicts.inc();
          throw new DomainError("IDEMPOTENCY_CONFLICT", 409, "Idempotency-Key was already used with another payload");
        }
        await client.query("COMMIT");
        metrics.idempotencyReplays.inc();
        return { statusCode: persisted.response_status, body: persisted.response_body, replayed: true };
      }

      const accountIds = deterministicAccountOrder([request.sourceAccountId, request.destinationAccountId]);
      await client.query(
        `INSERT INTO account_balances(account_id,currency)
         SELECT id,currency FROM ledger_accounts WHERE id=ANY($1::uuid[])
         ON CONFLICT DO NOTHING`, [accountIds],
      );
      const accountsResult = await client.query<Omit<AccountRow, "available_minor">>(
        `SELECT id,account_type,status,allow_negative,currency FROM ledger_accounts
         WHERE id=ANY($1::uuid[]) ORDER BY id FOR SHARE`, [accountIds],
      );
      if (accountsResult.rowCount !== 2) throw new DomainError("ACCOUNT_NOT_FOUND", 404, "source or destination account was not found");
      const balancesResult = await client.query<{ account_id: string; available_minor: bigint }>(
        `SELECT account_id,available_minor FROM account_balances
         WHERE account_id=ANY($1::uuid[]) ORDER BY account_id FOR UPDATE`, [accountIds],
      );
      const balances = new Map(balancesResult.rows.map((row) => [row.account_id, row.available_minor]));
      const byId = new Map(accountsResult.rows.map((row) => [row.id, { ...row, available_minor: balances.get(row.id) ?? 0n }]));
      const source = byId.get(request.sourceAccountId);
      const destination = byId.get(request.destinationAccountId);
      if (!source || !destination) throw new DomainError("ACCOUNT_NOT_FOUND", 404, "source or destination account was not found");
      for (const account of [source, destination]) {
        if (account.status !== "active") throw new DomainError("ACCOUNT_BLOCKED", 422, `account ${account.id} is blocked`);
        if (account.currency !== request.currency) throw new DomainError("CURRENCY_MISMATCH", 422, "account currency differs from transaction currency");
      }

      const sourceDebitNormal = source.account_type === "asset" || source.account_type === "expense";
      const destinationDebitNormal = destination.account_type === "asset" || destination.account_type === "expense";
      if (sourceDebitNormal !== destinationDebitNormal) {
        throw new DomainError("ACCOUNT_TYPE_MISMATCH", 422,
          "a two-posting transfer requires accounts with the same normal-balance side; use a multi-leg clearing workflow");
      }
      const sourceDirection = sourceDebitNormal ? "credit" as const : "debit" as const;
      const postings = transferPostings({
        sourceAccountId: request.sourceAccountId,
        destinationAccountId: request.destinationAccountId,
        amountMinor: BigInt(request.amountMinor),
        currency: request.currency,
        sourceDirection,
      });
      assertBalanced(postings);
      const sourceDelta = signedBalanceDelta(source.account_type, sourceDirection, BigInt(request.amountMinor));
      if (!source.allow_negative && source.available_minor + sourceDelta < 0n) {
        throw new DomainError("INSUFFICIENT_FUNDS", 422, "source account has insufficient funds");
      }

      await client.query(
        `INSERT INTO journal_entries(id,transaction_id,external_reference,status,occurred_at,created_at)
         VALUES ($1,$2,$3,'posted',$4,$4)`, [journalEntryId, transactionId, request.externalReference, createdAt],
      );
      for (const posting of postings) {
        await client.query(
          `INSERT INTO ledger_postings(journal_entry_id,account_id,direction,amount_minor,currency,sequence)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [journalEntryId, posting.accountId, posting.direction, posting.amountMinor, posting.currency, posting.sequence],
        );
        const account = byId.get(posting.accountId)!;
        const delta = signedBalanceDelta(account.account_type, posting.direction, posting.amountMinor);
        await client.query(
          `UPDATE account_balances SET available_minor=available_minor+$1,version=version+1,updated_at=now()
           WHERE account_id=$2 AND currency=$3`, [delta, posting.accountId, posting.currency],
        );
      }
      const eventId = randomUUID();
      await client.query(
        `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload,headers)
         VALUES ($1,'financial_transaction',$2,'financial.transaction.accepted.v1',1,$3,$4,$5)`,
        [eventId, transactionId, request.sourceAccountId,
          { eventId, transactionId, journalEntryId, ...request, occurredAt: createdAt },
          { correlationId: transactionId, schemaVersion: "1", ...(hooks.traceparent ? { traceparent: hooks.traceparent } : {}) }],
      );
      await hooks.beforeCommit?.();
      await client.query("COMMIT");
      metrics.transactionsCreated.inc(); metrics.ledgerEntries.inc();
      return { statusCode: 201, body: response, replayed: false };
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      const code = (error as { code?: string }).code;
      if ((code === "40001" || code === "40P01") && attempt < 2) {
        metrics.balanceRetries.inc({ sqlstate: code });
        await new Promise((resolve) => setTimeout(resolve, 5 * 2 ** attempt + Math.random() * 10));
        continue;
      }
      throw error;
    } finally {
      client.release();
    }
  }
  throw new Error("transaction retry limit exhausted");
}

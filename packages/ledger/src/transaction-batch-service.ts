import { randomUUID } from "node:crypto";
import type pg from "pg";
import type { TransactionRequest, TransactionResponse } from "../../contracts/src/transactions.js";
import { metrics } from "../../observability/src/metrics.js";
import { assertBalanced, deterministicAccountOrder, signedBalanceDelta, transferPostings, type AccountType } from "./domain.js";
import { requestHash } from "./idempotency.js";
import { DomainError, type TransactionResult } from "./transaction-service.js";

export interface BatchTransactionCommand {
  idempotencyKey: string;
  request: TransactionRequest;
  traceparent?: string;
}

export type BatchTransactionOutcome =
  | { ok: true; result: TransactionResult }
  | { ok: false; error: unknown };

interface AccountRow {
  id: string;
  account_type: AccountType;
  status: "active" | "blocked";
  allow_negative: boolean;
  currency: string;
}

interface PersistedTransaction {
  idempotency_key: string;
  request_hash: string;
  response_status: number;
  response_body: TransactionResponse;
}

interface PreparedCommand {
  index: number;
  id: string;
  journalEntryId: string;
  eventId: string;
  createdAt: string;
  hash: string;
  response: TransactionResponse;
  command: BatchTransactionCommand;
}

interface AcceptedCommand extends PreparedCommand {
  postings: ReturnType<typeof transferPostings>;
}

export interface BatchExecutionHooks {
  beforeCommit?: () => void | Promise<void>;
}

function rejected(error: unknown): BatchTransactionOutcome {
  return { ok: false, error };
}

function replayResult(row: PersistedTransaction): TransactionResult {
  return { statusCode: row.response_status, body: row.response_body, replayed: true };
}

function jsonRows(rows: unknown[]): string {
  return JSON.stringify(rows);
}

/**
 * Posts independent financial commands in one durable PostgreSQL transaction.
 * Domain failures are isolated as per-item outcomes; an infrastructure failure
 * rolls back and retries the complete batch.
 */
export async function createFinancialTransactionBatch(
  db: pg.Pool,
  commands: BatchTransactionCommand[],
  hooks: BatchExecutionHooks = {},
): Promise<BatchTransactionOutcome[]> {
  if (commands.length === 0) return [];
  const keys = new Set<string>();
  for (const command of commands) {
    if (keys.has(command.idempotencyKey)) throw new Error("batch commands must have unique idempotency keys");
    keys.add(command.idempotencyKey);
  }

  for (let attempt = 0; attempt < 3; attempt += 1) {
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      const outcomes = new Array<BatchTransactionOutcome | undefined>(commands.length);
      const prepared: PreparedCommand[] = [];

      for (const [index, command] of commands.entries()) {
        if (!command.idempotencyKey || command.idempotencyKey.length > 128) {
          outcomes[index] = rejected(new DomainError("INVALID_IDEMPOTENCY_KEY", 400,
            "Idempotency-Key is required and must have at most 128 characters"));
          continue;
        }
        if (command.request.sourceAccountId === command.request.destinationAccountId) {
          outcomes[index] = rejected(new DomainError("INVALID_ACCOUNT_PAIR", 422,
            "source and destination accounts must differ"));
          continue;
        }
        const id = randomUUID();
        const journalEntryId = randomUUID();
        const createdAt = new Date().toISOString();
        prepared.push({
          index,
          id,
          journalEntryId,
          eventId: randomUUID(),
          createdAt,
          hash: requestHash(command.request),
          command,
          response: {
            id,
            journalEntryId,
            externalReference: command.request.externalReference,
            status: "accepted",
            amountMinor: command.request.amountMinor,
            currency: command.request.currency,
            createdAt,
          },
        });
      }

      const existingByKey = new Map<string, PersistedTransaction>();
      if (prepared.length > 0) {
        const existing = await client.query<PersistedTransaction>(
          `SELECT idempotency_key,request_hash,response_status,response_body
           FROM financial_transactions WHERE idempotency_key=ANY($1::text[])`,
          [prepared.map((item) => item.command.idempotencyKey)],
        );
        for (const row of existing.rows) existingByKey.set(row.idempotency_key, row);
      }

      const absent: PreparedCommand[] = [];
      for (const item of prepared) {
        const existing = existingByKey.get(item.command.idempotencyKey);
        if (!existing) {
          absent.push(item);
        } else if (existing.request_hash === item.hash) {
          outcomes[item.index] = { ok: true, result: replayResult(existing) };
        } else {
          outcomes[item.index] = rejected(new DomainError("IDEMPOTENCY_CONFLICT", 409,
            "Idempotency-Key was already used with another payload"));
        }
      }

      const accountIds = deterministicAccountOrder(absent.flatMap((item) => [
        item.command.request.sourceAccountId,
        item.command.request.destinationAccountId,
      ]));
      const accountsById = new Map<string, AccountRow>();
      if (accountIds.length > 0) {
        const accounts = await client.query<AccountRow>(
          `SELECT id,account_type,status,allow_negative,currency FROM ledger_accounts
           WHERE id=ANY($1::uuid[]) ORDER BY id FOR SHARE`,
          [accountIds],
        );
        for (const row of accounts.rows) accountsById.set(row.id, row);
      }

      const structurallyValid: PreparedCommand[] = [];
      for (const item of absent) {
        const request = item.command.request;
        const source = accountsById.get(request.sourceAccountId);
        const destination = accountsById.get(request.destinationAccountId);
        if (!source || !destination) {
          outcomes[item.index] = rejected(new DomainError("ACCOUNT_NOT_FOUND", 404,
            "source or destination account was not found"));
          continue;
        }
        if (source.status !== "active" || destination.status !== "active") {
          const blocked = source.status !== "active" ? source.id : destination.id;
          outcomes[item.index] = rejected(new DomainError("ACCOUNT_BLOCKED", 422, `account ${blocked} is blocked`));
          continue;
        }
        if (source.currency !== request.currency || destination.currency !== request.currency) {
          outcomes[item.index] = rejected(new DomainError("CURRENCY_MISMATCH", 422,
            "account currency differs from transaction currency"));
          continue;
        }
        const sourceDebitNormal = source.account_type === "asset" || source.account_type === "expense";
        const destinationDebitNormal = destination.account_type === "asset" || destination.account_type === "expense";
        if (sourceDebitNormal !== destinationDebitNormal) {
          outcomes[item.index] = rejected(new DomainError("ACCOUNT_TYPE_MISMATCH", 422,
            "a two-posting transfer requires accounts with the same normal-balance side; use a multi-leg clearing workflow"));
          continue;
        }
        structurallyValid.push(item);
      }

      const insertedKeys = new Set<string>();
      if (structurallyValid.length > 0) {
        const inserted = await client.query<{ idempotency_key: string }>(
          `INSERT INTO financial_transactions(
             id,idempotency_key,external_reference,source_account_id,destination_account_id,
             amount_minor,currency,status,request_hash,response_status,response_body,created_at,end_to_end_id,provider_transaction_id)
           SELECT x.id,x.idempotency_key,x.external_reference,x.source_account_id,x.destination_account_id,
             x.amount_minor,x.currency,'accepted',x.request_hash,201,x.response_body,x.created_at,x.end_to_end_id,x.provider_transaction_id
           FROM jsonb_to_recordset($1::jsonb) AS x(
             id uuid,idempotency_key text,external_reference text,source_account_id uuid,destination_account_id uuid,
             amount_minor bigint,currency text,request_hash text,response_body jsonb,created_at timestamptz,
             end_to_end_id text,provider_transaction_id text)
           ON CONFLICT (idempotency_key) DO NOTHING
           RETURNING idempotency_key`,
          [jsonRows(structurallyValid.map((item) => ({
            id: item.id,
            idempotency_key: item.command.idempotencyKey,
            external_reference: item.command.request.externalReference,
            source_account_id: item.command.request.sourceAccountId,
            destination_account_id: item.command.request.destinationAccountId,
            amount_minor: item.command.request.amountMinor,
            currency: item.command.request.currency,
            request_hash: item.hash,
            response_body: item.response,
            created_at: item.createdAt,
            end_to_end_id: item.command.request.endToEndId ?? null,
            provider_transaction_id: item.command.request.providerTransactionId ?? null,
          })))],
        );
        for (const row of inserted.rows) insertedKeys.add(row.idempotency_key);
      }

      const lostRace = structurallyValid.filter((item) => !insertedKeys.has(item.command.idempotencyKey));
      if (lostRace.length > 0) {
        const winners = await client.query<PersistedTransaction>(
          `SELECT idempotency_key,request_hash,response_status,response_body
           FROM financial_transactions WHERE idempotency_key=ANY($1::text[])`,
          [lostRace.map((item) => item.command.idempotencyKey)],
        );
        const winnerByKey = new Map(winners.rows.map((row) => [row.idempotency_key, row]));
        for (const item of lostRace) {
          const winner = winnerByKey.get(item.command.idempotencyKey);
          if (!winner) throw new Error("idempotency winner disappeared");
          outcomes[item.index] = winner.request_hash === item.hash
            ? { ok: true, result: replayResult(winner) }
            : rejected(new DomainError("IDEMPOTENCY_CONFLICT", 409,
              "Idempotency-Key was already used with another payload"));
        }
      }

      const inserted = structurallyValid.filter((item) => insertedKeys.has(item.command.idempotencyKey));
      const insertedAccountIds = deterministicAccountOrder(inserted.flatMap((item) => [
        item.command.request.sourceAccountId,
        item.command.request.destinationAccountId,
      ]));
      if (insertedAccountIds.length > 0) {
        await client.query(
          `INSERT INTO account_balances(account_id,currency)
           SELECT id,currency FROM ledger_accounts WHERE id=ANY($1::uuid[])
           ON CONFLICT DO NOTHING`,
          [insertedAccountIds],
        );
      }

      const balances = new Map<string, bigint>();
      if (insertedAccountIds.length > 0) {
        const locked = await client.query<{ account_id: string; available_minor: bigint }>(
          `SELECT account_id,available_minor FROM account_balances
           WHERE account_id=ANY($1::uuid[]) ORDER BY account_id FOR UPDATE`,
          [insertedAccountIds],
        );
        for (const row of locked.rows) balances.set(row.account_id, row.available_minor);
      }

      const accepted: AcceptedCommand[] = [];
      const rejectedIds: string[] = [];
      for (const item of inserted) {
        const request = item.command.request;
        const source = accountsById.get(request.sourceAccountId)!;
        const destination = accountsById.get(request.destinationAccountId)!;
        const sourceDebitNormal = source.account_type === "asset" || source.account_type === "expense";
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
        const currentSource = balances.get(source.id) ?? 0n;
        if (!source.allow_negative && currentSource + sourceDelta < 0n) {
          outcomes[item.index] = rejected(new DomainError("INSUFFICIENT_FUNDS", 422,
            "source account has insufficient funds"));
          rejectedIds.push(item.id);
          continue;
        }
        for (const posting of postings) {
          const account = accountsById.get(posting.accountId)!;
          const delta = signedBalanceDelta(account.account_type, posting.direction, posting.amountMinor);
          balances.set(posting.accountId, (balances.get(posting.accountId) ?? 0n) + delta);
        }
        accepted.push({ ...item, postings });
      }

      if (rejectedIds.length > 0) {
        await client.query("DELETE FROM financial_transactions WHERE id=ANY($1::uuid[])", [rejectedIds]);
      }

      if (accepted.length > 0) {
        await client.query(
          `INSERT INTO journal_entries(id,transaction_id,external_reference,status,occurred_at,created_at)
           SELECT x.id,x.transaction_id,x.external_reference,'posted',x.created_at,x.created_at
           FROM jsonb_to_recordset($1::jsonb) AS x(
             id uuid,transaction_id uuid,external_reference text,created_at timestamptz)`,
          [jsonRows(accepted.map((item) => ({
            id: item.journalEntryId,
            transaction_id: item.id,
            external_reference: item.command.request.externalReference,
            created_at: item.createdAt,
          })))],
        );

        const postingRows = accepted.flatMap((item) => item.postings.map((posting) => ({
          journal_entry_id: item.journalEntryId,
          account_id: posting.accountId,
          direction: posting.direction,
          amount_minor: posting.amountMinor.toString(),
          currency: posting.currency,
          sequence: posting.sequence,
        })));
        await client.query(
          `INSERT INTO ledger_postings(journal_entry_id,account_id,direction,amount_minor,currency,sequence)
           SELECT x.journal_entry_id,x.account_id,x.direction,x.amount_minor,x.currency,x.sequence
           FROM jsonb_to_recordset($1::jsonb) AS x(
             journal_entry_id uuid,account_id uuid,direction text,amount_minor bigint,currency text,sequence integer)`,
          [jsonRows(postingRows)],
        );

        const deltas = new Map<string, { accountId: string; currency: string; delta: bigint; versions: bigint }>();
        for (const item of accepted) {
          for (const posting of item.postings) {
            const account = accountsById.get(posting.accountId)!;
            const delta = signedBalanceDelta(account.account_type, posting.direction, posting.amountMinor);
            const key = `${posting.accountId}:${posting.currency}`;
            const current = deltas.get(key) ?? {
              accountId: posting.accountId, currency: posting.currency, delta: 0n, versions: 0n,
            };
            current.delta += delta;
            current.versions += 1n;
            deltas.set(key, current);
          }
        }
        await client.query(
          `UPDATE account_balances b SET
             available_minor=b.available_minor+x.delta_minor,
             version=b.version+x.version_increment,
             updated_at=now()
           FROM jsonb_to_recordset($1::jsonb) AS x(
             account_id uuid,currency text,delta_minor bigint,version_increment bigint)
           WHERE b.account_id=x.account_id AND b.currency=x.currency`,
          [jsonRows([...deltas.values()].map((delta) => ({
            account_id: delta.accountId,
            currency: delta.currency,
            delta_minor: delta.delta.toString(),
            version_increment: delta.versions.toString(),
          })))],
        );

        await client.query(
          `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload,headers)
           SELECT x.id,'financial_transaction',x.aggregate_id,'financial.transaction.accepted.v1',1,
             x.partition_key,x.payload,x.headers
           FROM jsonb_to_recordset($1::jsonb) AS x(
             id uuid,aggregate_id uuid,partition_key text,payload jsonb,headers jsonb)`,
          [jsonRows(accepted.map((item) => ({
            id: item.eventId,
            aggregate_id: item.id,
            partition_key: item.command.request.sourceAccountId,
            payload: {
              eventId: item.eventId,
              transactionId: item.id,
              journalEntryId: item.journalEntryId,
              ...item.command.request,
              occurredAt: item.createdAt,
            },
            headers: {
              correlationId: item.id,
              schemaVersion: "1",
              ...(item.command.traceparent ? { traceparent: item.command.traceparent } : {}),
            },
          })))],
        );
      }

      await hooks.beforeCommit?.();
      await client.query("COMMIT");

      for (const item of accepted) {
        outcomes[item.index] = { ok: true, result: { statusCode: 201, body: item.response, replayed: false } };
      }
      for (const outcome of outcomes) {
        if (!outcome) throw new Error("batch outcome was not resolved");
        if (outcome.ok) {
          if (outcome.result.replayed) metrics.idempotencyReplays.inc();
        } else if (outcome.error instanceof DomainError && outcome.error.code === "IDEMPOTENCY_CONFLICT") {
          metrics.idempotencyConflicts.inc();
        }
      }
      if (accepted.length > 0) {
        metrics.transactionsCreated.inc(accepted.length);
        metrics.ledgerEntries.inc(accepted.length);
      }
      return outcomes as BatchTransactionOutcome[];
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
  throw new Error("batch transaction retry limit exhausted");
}

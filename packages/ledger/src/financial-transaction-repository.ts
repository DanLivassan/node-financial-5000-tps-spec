import type pg from "pg";
import type { PreparedCommand } from "./batch-command-validator.js";

function jsonRows(rows: unknown[]): string { return JSON.stringify(rows); }

export async function reserveFinancialTransactions(
  client: pg.PoolClient, items: PreparedCommand[],
): Promise<Set<string>> {
  if (items.length === 0) return new Set();
  const result = await client.query<{ idempotency_key: string }>(
    `INSERT INTO financial_transactions(
       id,idempotency_key,external_reference,source_account_id,destination_account_id,
       amount_minor,currency,status,request_hash,response_status,response_body,created_at,end_to_end_id,provider_transaction_id)
     SELECT x.id,x.idempotency_key,x.external_reference,x.source_account_id,x.destination_account_id,
       x.amount_minor,x.currency,'accepted',x.request_hash,201,x.response_body,x.created_at,x.end_to_end_id,x.provider_transaction_id
     FROM jsonb_to_recordset($1::jsonb) AS x(
       id uuid,idempotency_key text,external_reference text,source_account_id uuid,destination_account_id uuid,
       amount_minor bigint,currency text,request_hash text,response_body jsonb,created_at timestamptz,
       end_to_end_id text,provider_transaction_id text)
     ON CONFLICT (idempotency_key) DO NOTHING RETURNING idempotency_key`,
    [jsonRows(items.map((item) => ({
      id: item.id, idempotency_key: item.command.idempotencyKey,
      external_reference: item.command.request.externalReference,
      source_account_id: item.command.request.sourceAccountId,
      destination_account_id: item.command.request.destinationAccountId,
      amount_minor: item.command.request.amountMinor, currency: item.command.request.currency,
      request_hash: item.hash, response_body: item.response, created_at: item.createdAt,
      end_to_end_id: item.command.request.endToEndId ?? null,
      provider_transaction_id: item.command.request.providerTransactionId ?? null,
    })))],
  );
  return new Set(result.rows.map((row) => row.idempotency_key));
}

export async function deleteRejectedTransactions(client: pg.PoolClient, ids: string[]): Promise<void> {
  if (ids.length > 0) await client.query("DELETE FROM financial_transactions WHERE id=ANY($1::uuid[])", [ids]);
}

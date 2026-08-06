import type pg from "pg";
import type { AcceptedCommand } from "./batch-simulator.js";

function jsonRows(rows: unknown[]): string { return JSON.stringify(rows); }

export async function persistLedger(client: pg.PoolClient, accepted: AcceptedCommand[]): Promise<void> {
  if (accepted.length === 0) return;
  await client.query(
    `INSERT INTO journal_entries(id,transaction_id,external_reference,status,occurred_at,created_at)
     SELECT x.id,x.transaction_id,x.external_reference,'posted',x.created_at,x.created_at
     FROM jsonb_to_recordset($1::jsonb) AS x(
       id uuid,transaction_id uuid,external_reference text,created_at timestamptz)`,
    [jsonRows(accepted.map((item) => ({
      id: item.journalEntryId, transaction_id: item.id,
      external_reference: item.command.request.externalReference, created_at: item.createdAt,
    })))],
  );
  const rows = accepted.flatMap((item) => item.postings.map((posting) => ({
    journal_entry_id: item.journalEntryId, account_id: posting.accountId,
    direction: posting.direction, amount_minor: posting.amountMinor.toString(),
    currency: posting.currency, sequence: posting.sequence,
  })));
  await client.query(
    `INSERT INTO ledger_postings(journal_entry_id,account_id,direction,amount_minor,currency,sequence)
     SELECT x.journal_entry_id,x.account_id,x.direction,x.amount_minor,x.currency,x.sequence
     FROM jsonb_to_recordset($1::jsonb) AS x(
       journal_entry_id uuid,account_id uuid,direction text,amount_minor bigint,currency text,sequence integer)`,
    [jsonRows(rows)],
  );
}

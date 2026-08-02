import { randomUUID } from "node:crypto";
import type pg from "pg";
import { deterministicAccountOrder, signedBalanceDelta, type AccountType, type Direction } from "./domain.js";
import { DomainError } from "./transaction-service.js";

interface OriginalPosting { account_id: string; direction: Direction; amount_minor: bigint; currency: string; sequence: number; account_type: AccountType; }

export async function createCompensatingEntry(db: pg.Pool, transactionId: string): Promise<{ journalEntryId: string; replayed: boolean }> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const original = await client.query<{ journal_id: string; external_reference: string; status: string }>(
      `SELECT j.id journal_id,j.external_reference,t.status FROM journal_entries j
       JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.id=$1 FOR UPDATE OF t`, [transactionId],
    );
    const source = original.rows[0];
    if (!source) throw new DomainError("TRANSACTION_NOT_FOUND", 404, "transaction was not found");
    const already = await client.query<{ id: string }>("SELECT id FROM journal_entries WHERE reversal_of_journal_id=$1", [source.journal_id]);
    if (already.rows[0]) { await client.query("COMMIT"); return { journalEntryId: already.rows[0].id, replayed: true }; }
    const postingsResult = await client.query<OriginalPosting>(
      `SELECT p.account_id,p.direction,p.amount_minor,p.currency,p.sequence,a.account_type
       FROM ledger_postings p JOIN ledger_accounts a ON a.id=p.account_id
       WHERE p.journal_entry_id=$1 ORDER BY p.sequence`, [source.journal_id],
    );
    const accountIds = deterministicAccountOrder(postingsResult.rows.map((row) => row.account_id));
    await client.query("SELECT account_id FROM account_balances WHERE account_id=ANY($1::uuid[]) ORDER BY account_id FOR UPDATE", [accountIds]);
    const journalEntryId = randomUUID();
    await client.query(
      `INSERT INTO journal_entries(id,reversal_of_journal_id,external_reference,status,occurred_at)
       VALUES ($1,$2,$3,'reversal',now())`, [journalEntryId, source.journal_id, `reversal:${source.external_reference}`],
    );
    for (const posting of postingsResult.rows) {
      const direction: Direction = posting.direction === "debit" ? "credit" : "debit";
      await client.query(
        `INSERT INTO ledger_postings(journal_entry_id,account_id,direction,amount_minor,currency,sequence)
         VALUES ($1,$2,$3,$4,$5,$6)`, [journalEntryId, posting.account_id, direction, posting.amount_minor, posting.currency, posting.sequence],
      );
      await client.query(
        `UPDATE account_balances SET available_minor=available_minor+$1,version=version+1,updated_at=now()
         WHERE account_id=$2 AND currency=$3`,
        [signedBalanceDelta(posting.account_type, direction, posting.amount_minor), posting.account_id, posting.currency],
      );
    }
    await client.query("UPDATE financial_transactions SET status='reversed' WHERE id=$1", [transactionId]);
    const eventId = randomUUID();
    await client.query(
      `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload)
       VALUES ($1,'financial_transaction',$2::uuid,'financial.transaction.reversed.v1',1,$2::text,$3)`,
      [eventId, transactionId, { eventId, transactionId, journalEntryId, reversedJournalEntryId: source.journal_id }],
    );
    await client.query("COMMIT");
    return { journalEntryId, replayed: false };
  } catch (error) { await client.query("ROLLBACK").catch(() => undefined); throw error; }
  finally { client.release(); }
}

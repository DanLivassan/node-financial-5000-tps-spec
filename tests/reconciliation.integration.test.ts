import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { pool } from "../packages/database/src/pool.js";
import { importBankStatement, runReconciliation } from "../packages/ledger/src/reconciliation.js";
import { createCompensatingEntry } from "../packages/ledger/src/reversal-service.js";
import { createFinancialTransaction } from "../packages/ledger/src/transaction-service.js";

const funding = randomUUID();
const source = randomUUID();
const destination = randomUUID();
const provider = `provider-${randomUUID()}`;
const bankAccountId = `bank-${randomUUID()}`;
const targetKey = `recon-tx-${randomUUID()}`;
const periodStart = new Date(Date.now() - 60_000).toISOString();
const periodEnd = new Date(Date.now() + 60_000).toISOString();
let targetTransactionId: string;

beforeAll(async () => {
  await pool.query(
    `INSERT INTO ledger_accounts(id,code,account_type,currency,allow_negative) VALUES
     ($1,$2,'equity','BRL',true),($3,$4,'liability','BRL',false),($5,$6,'liability','BRL',false)`,
    [funding, `recon-fund-${funding}`, source, `recon-source-${source}`, destination, `recon-dest-${destination}`],
  );
  await createFinancialTransaction(pool, `recon-funding-${source}`, {
    externalReference: `fund-${source}`, sourceAccountId: funding, destinationAccountId: source,
    amountMinor: 10_000, currency: "BRL",
  });
  const target = await createFinancialTransaction(pool, targetKey, {
    externalReference: `order-${targetKey}`, endToEndId: `e2e-${targetKey}`,
    providerTransactionId: `provider-tx-${targetKey}`, sourceAccountId: source, destinationAccountId: destination,
    amountMinor: 100, currency: "BRL",
  });
  targetTransactionId = target.body.id;
});

afterAll(() => pool.end());

describe("bank reconciliation", () => {
  it("imports and reruns safely, preserving raw entries", async () => {
    const entries = [
      { providerEntryId: "bank-entry-1", endToEndId: `e2e-${targetKey}`, providerTransactionId: `provider-tx-${targetKey}`,
        externalReference: `order-${targetKey}`, direction: "credit" as const, amountMinor: 100, currency: "BRL",
        occurredAt: new Date().toISOString(), rawPayload: { original: true } },
      { providerEntryId: "bank-entry-unmatched", endToEndId: `missing-${targetKey}`,
        direction: "credit" as const, amountMinor: 50, currency: "BRL",
        occurredAt: new Date().toISOString(), rawPayload: { original: "unmatched" } },
    ];
    expect(await importBankStatement(pool, provider, bankAccountId, entries)).toEqual({ inserted: 2, duplicates: 0 });
    expect(await importBankStatement(pool, provider, bankAccountId, entries)).toEqual({ inserted: 0, duplicates: 2 });
    const input = { provider, bankAccountId, ledgerAccountId: destination, periodStart, periodEnd,
      bankOpeningMinor: 0, bankClosingMinor: 150 };
    const first = await runReconciliation(pool, input);
    expect(first.replayed).toBe(false);
    expect(first.items.map((item) => item.status).sort()).toEqual(["matched", "missing_in_ledger"]);
    const replay = await runReconciliation(pool, input);
    expect(replay).toMatchObject({ runId: first.runId, replayed: true });
    expect(replay.items).toHaveLength(first.items.length);
    const proof = await pool.query<{ runs: bigint; items: bigint; events: bigint }>(
      `SELECT (SELECT count(*) FROM reconciliation_runs WHERE id=$1) runs,
       (SELECT count(*) FROM reconciliation_items WHERE reconciliation_run_id=$1) items,
       (SELECT count(*) FROM outbox_events WHERE aggregate_id=$1) events`, [first.runId],
    );
    expect(proof.rows[0]).toEqual({ runs: 1n, items: 2n, events: 2n });
  });

  it("creates one idempotent compensating journal instead of mutating postings", async () => {
    const original = await pool.query<{ count: bigint }>(
      `SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id WHERE j.transaction_id=$1`, [targetTransactionId],
    );
    const first = await createCompensatingEntry(pool, targetTransactionId);
    const replay = await createCompensatingEntry(pool, targetTransactionId);
    expect(replay).toEqual({ journalEntryId: first.journalEntryId, replayed: true });
    const proof = await pool.query<{ original_postings: bigint; reversal_postings: bigint; status: string; destination_balance: bigint }>(
      `SELECT
       (SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id WHERE j.transaction_id=$1) original_postings,
       (SELECT count(*) FROM ledger_postings WHERE journal_entry_id=$2) reversal_postings,
       (SELECT status FROM financial_transactions WHERE id=$1) status,
       (SELECT available_minor FROM account_balances WHERE account_id=$3 AND currency='BRL') destination_balance`,
      [targetTransactionId, first.journalEntryId, destination],
    );
    expect(proof.rows[0]).toEqual({ original_postings: original.rows[0]!.count, reversal_postings: 2n, status: "reversed", destination_balance: 0n });
  });
});

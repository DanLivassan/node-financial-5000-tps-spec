import { createHash, randomUUID } from "node:crypto";
import type pg from "pg";
import { metrics } from "../../observability/src/metrics.js";

export type ReconciliationStatus = "matched" | "missing_at_bank" | "missing_in_ledger" |
  "amount_mismatch" | "direction_mismatch" | "duplicate_bank_entry" |
  "duplicate_internal_entry" | "late_settlement" | "manual_review";

export interface Movement {
  id: string;
  endToEndId: string | null;
  providerTransactionId: string | null;
  externalReference: string | null;
  direction: "debit" | "credit";
  amountMinor: bigint;
  currency: string;
  occurredAt: Date;
}

export interface MatchItem {
  transactionId: string | null;
  bankStatementEntryId: string | null;
  matchKey: string | null;
  status: ReconciliationStatus;
  differenceMinor: bigint;
  reason: string;
}

function identifier(movement: Movement): string | null {
  if (movement.endToEndId) return `end_to_end:${movement.endToEndId}`;
  if (movement.providerTransactionId) return `provider_transaction:${movement.providerTransactionId}`;
  if (movement.externalReference) return `external_reference:${movement.externalReference}`;
  return null;
}

export function matchMovements(internal: readonly Movement[], bank: readonly Movement[], lateWindowMs = 3 * 86_400_000): MatchItem[] {
  const items: MatchItem[] = [];
  const usedInternal = new Set<string>();
  const seenBankKeys = new Set<string>();
  for (const bankEntry of bank) {
    const keys: Array<[string, string | null]> = [
      ["end_to_end", bankEntry.endToEndId],
      ["provider_transaction", bankEntry.providerTransactionId],
      ["external_reference", bankEntry.externalReference],
    ];
    let matchKey: string | null = null;
    let candidates: Movement[] = [];
    for (const [kind, value] of keys) {
      if (!value) continue;
      const found = internal.filter((entry) => {
        if (kind === "end_to_end") return entry.endToEndId === value;
        if (kind === "provider_transaction") return entry.providerTransactionId === value;
        return entry.externalReference === value;
      });
      if (found.length > 0) { matchKey = `${kind}:${value}`; candidates = found; break; }
    }
    const bankIdentity = identifier(bankEntry);
    if (bankIdentity && seenBankKeys.has(bankIdentity)) {
      items.push({ transactionId: null, bankStatementEntryId: bankEntry.id, matchKey: bankIdentity,
        status: "duplicate_bank_entry", differenceMinor: bankEntry.amountMinor, reason: "repeated strong bank identifier" });
      continue;
    }
    if (bankIdentity) seenBankKeys.add(bankIdentity);
    if (candidates.length > 1) {
      items.push({ transactionId: null, bankStatementEntryId: bankEntry.id, matchKey,
        status: "duplicate_internal_entry", differenceMinor: bankEntry.amountMinor, reason: "strong identifier maps to multiple internal movements" });
      continue;
    }
    const candidate = candidates[0];
    if (!candidate) {
      items.push({ transactionId: null, bankStatementEntryId: bankEntry.id, matchKey: bankIdentity,
        status: bankIdentity ? "missing_in_ledger" : "manual_review", differenceMinor: bankEntry.amountMinor,
        reason: bankIdentity ? "no internal movement with strong identifier" : "no strong identifier; amount-only matching is forbidden" });
      continue;
    }
    if (usedInternal.has(candidate.id)) {
      items.push({ transactionId: candidate.id, bankStatementEntryId: bankEntry.id, matchKey,
        status: "duplicate_bank_entry", differenceMinor: bankEntry.amountMinor, reason: "internal movement already matched" });
      continue;
    }
    usedInternal.add(candidate.id);
    let status: ReconciliationStatus = "matched";
    let reason = "strong identifier and secondary evidence agree";
    if (candidate.direction !== bankEntry.direction) { status = "direction_mismatch"; reason = "direction differs"; }
    else if (candidate.currency !== bankEntry.currency) { status = "manual_review"; reason = "currency differs"; }
    else if (candidate.amountMinor !== bankEntry.amountMinor) { status = "amount_mismatch"; reason = "amount differs"; }
    else if (Math.abs(candidate.occurredAt.getTime() - bankEntry.occurredAt.getTime()) > lateWindowMs) {
      status = "late_settlement"; reason = "settlement is outside configured time window";
    }
    items.push({ transactionId: candidate.id, bankStatementEntryId: bankEntry.id, matchKey, status,
      differenceMinor: bankEntry.amountMinor - candidate.amountMinor, reason });
  }
  for (const movement of internal) {
    if (!usedInternal.has(movement.id)) items.push({ transactionId: movement.id, bankStatementEntryId: null,
      matchKey: identifier(movement), status: "missing_at_bank", differenceMinor: -movement.amountMinor,
      reason: "internal movement has no bank counterpart" });
  }
  return items;
}

export function closingBalance(opening: bigint, movements: readonly Pick<Movement, "direction" | "amountMinor">[]): bigint {
  return movements.reduce((balance, movement) => balance + (movement.direction === "credit" ? movement.amountMinor : -movement.amountMinor), opening);
}

export interface BankEntryInput {
  providerEntryId: string;
  providerTransactionId?: string;
  endToEndId?: string;
  externalReference?: string;
  direction: "debit" | "credit";
  amountMinor: number;
  currency: string;
  occurredAt: string;
  rawPayload: unknown;
}

export async function importBankStatement(db: pg.Pool, provider: string, bankAccountId: string, entries: readonly BankEntryInput[]) {
  const client = await db.connect();
  let inserted = 0;
  try {
    await client.query("BEGIN");
    for (const entry of entries) {
      const result = await client.query(
        `INSERT INTO bank_statement_entries(provider,bank_account_id,provider_entry_id,provider_transaction_id,
          end_to_end_id,external_reference,direction,amount_minor,currency,occurred_at,raw_payload)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (provider,bank_account_id,provider_entry_id) DO NOTHING`,
        [provider, bankAccountId, entry.providerEntryId, entry.providerTransactionId ?? null, entry.endToEndId ?? null,
          entry.externalReference ?? null, entry.direction, entry.amountMinor, entry.currency, entry.occurredAt, entry.rawPayload],
      );
      inserted += result.rowCount ?? 0;
    }
    await client.query("COMMIT");
    return { inserted, duplicates: entries.length - inserted };
  } catch (error) {
    await client.query("ROLLBACK"); throw error;
  } finally { client.release(); }
}

export interface ReconciliationInput {
  provider: string;
  bankAccountId: string;
  ledgerAccountId: string;
  periodStart: string;
  periodEnd: string;
  bankOpeningMinor: number;
  bankClosingMinor: number;
}

export async function runReconciliation(db: pg.Pool, input: ReconciliationInput): Promise<{ runId: string; replayed: boolean; items: MatchItem[] }> {
  const runKey = createHash("sha256").update(JSON.stringify(input)).digest("hex");
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const existing = await client.query<{ id: string; status: string }>("SELECT id,status FROM reconciliation_runs WHERE run_key=$1 FOR UPDATE", [runKey]);
    if (existing.rows[0]?.status === "completed") {
      const stored = await client.query<any>(`SELECT transaction_id,bank_statement_entry_id,match_key,status,difference_minor,reason
        FROM reconciliation_items WHERE reconciliation_run_id=$1 ORDER BY created_at,id`, [existing.rows[0].id]);
      await client.query("COMMIT");
      return { runId: existing.rows[0].id, replayed: true, items: stored.rows.map(toMatchItem) };
    }
    const runId = existing.rows[0]?.id ?? randomUUID();
    if (!existing.rows[0]) await client.query(
      `INSERT INTO reconciliation_runs(id,run_key,provider,bank_account_id,ledger_account_id,period_start,period_end,status,bank_opening_minor,bank_closing_minor)
       VALUES ($1,$2,$3,$4,$5,$6,$7,'running',$8,$9)`,
      [runId, runKey, input.provider, input.bankAccountId, input.ledgerAccountId, input.periodStart, input.periodEnd,
        input.bankOpeningMinor, input.bankClosingMinor],
    );
    else await client.query("DELETE FROM reconciliation_items WHERE reconciliation_run_id=$1", [runId]);

    const internalRows = await client.query<any>(
      `SELECT t.id,t.end_to_end_id,t.provider_transaction_id,t.external_reference,p.direction,p.amount_minor,p.currency,j.occurred_at
       FROM financial_transactions t JOIN journal_entries j ON j.transaction_id=t.id
       JOIN ledger_postings p ON p.journal_entry_id=j.id
       WHERE p.account_id=$1 AND j.occurred_at >= $2 AND j.occurred_at < $3 AND t.status='accepted' ORDER BY j.occurred_at,t.id`,
      [input.ledgerAccountId, input.periodStart, input.periodEnd],
    );
    const bankRows = await client.query<any>(
      `SELECT id,end_to_end_id,provider_transaction_id,external_reference,direction,amount_minor,currency,occurred_at
       FROM bank_statement_entries WHERE provider=$1 AND bank_account_id=$2 AND occurred_at >= $3 AND occurred_at < $4
       ORDER BY occurred_at,id`, [input.provider, input.bankAccountId, input.periodStart, input.periodEnd],
    );
    const internal = internalRows.rows.map(toMovement);
    const bank = bankRows.rows.map(toMovement);
    const items = matchMovements(internal, bank);
    const calculatedBankClosing = closingBalance(BigInt(input.bankOpeningMinor), bank);
    if (calculatedBankClosing !== BigInt(input.bankClosingMinor)) items.push({
      transactionId: null, bankStatementEntryId: null, matchKey: "period_balance_formula",
      status: "manual_review", differenceMinor: BigInt(input.bankClosingMinor) - calculatedBankClosing,
      reason: "bank opening + credits - debits does not equal reported closing",
    });
    for (const item of items) await client.query(
      `INSERT INTO reconciliation_items(reconciliation_run_id,transaction_id,bank_statement_entry_id,match_key,status,difference_minor,reason)
       VALUES ($1,$2,$3,$4,$5,$6,$7)`, [runId, item.transactionId, item.bankStatementEntryId, item.matchKey, item.status, item.differenceMinor, item.reason],
    );
    const balanceResult = await client.query<{ opening: bigint; closing: bigint }>(
      `SELECT COALESCE(sum(CASE
        WHEN j.occurred_at >= $2 THEN 0 WHEN a.account_type IN ('asset','expense') AND p.direction='debit' THEN p.amount_minor
        WHEN j.occurred_at >= $2 THEN 0 WHEN a.account_type IN ('asset','expense') THEN -p.amount_minor
        WHEN j.occurred_at >= $2 THEN 0 WHEN p.direction='credit' THEN p.amount_minor ELSE -p.amount_minor END),0)::bigint opening,
        COALESCE(sum(CASE
        WHEN j.occurred_at >= $3 THEN 0 WHEN a.account_type IN ('asset','expense') AND p.direction='debit' THEN p.amount_minor
        WHEN j.occurred_at >= $3 THEN 0 WHEN a.account_type IN ('asset','expense') THEN -p.amount_minor
        WHEN j.occurred_at >= $3 THEN 0 WHEN p.direction='credit' THEN p.amount_minor ELSE -p.amount_minor END),0)::bigint closing
       FROM ledger_accounts a LEFT JOIN ledger_postings p ON p.account_id=a.id
       LEFT JOIN journal_entries j ON j.id=p.journal_entry_id WHERE a.id=$1 GROUP BY a.id`,
      [input.ledgerAccountId, input.periodStart, input.periodEnd],
    );
    const internalOpening = balanceResult.rows[0]?.opening ?? 0n;
    const internalClosing = balanceResult.rows[0]?.closing ?? 0n;
    const counts = { matched: items.filter((x) => x.status === "matched").length,
      missingInternal: items.filter((x) => x.status === "missing_at_bank").length,
      missingBank: items.filter((x) => x.status === "missing_in_ledger").length,
      mismatch: items.filter((x) => !["matched","missing_at_bank","missing_in_ledger"].includes(x.status)).length };
    await client.query(
      `UPDATE reconciliation_runs SET status='completed',internal_opening_minor=$2,internal_closing_minor=$3,
       matched_count=$4,unmatched_internal_count=$5,unmatched_bank_count=$6,mismatch_count=$7,finished_at=now() WHERE id=$1`,
      [runId, internalOpening, internalClosing, counts.matched, counts.missingInternal, counts.missingBank, counts.mismatch],
    );
    const summaryEventId = randomUUID();
    await client.query(
      `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload)
       VALUES ($1,'reconciliation_run',$2,'financial.reconciliation.completed.v1',1,$3,$4)`,
      [summaryEventId, runId, input.ledgerAccountId, { eventId: summaryEventId, runId, ...counts }],
    );
    if (items.some((item) => item.status !== "matched")) {
      const divergenceEventId = randomUUID();
      await client.query(
        `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload)
         VALUES ($1,'reconciliation_run',$2,'financial.reconciliation.divergence-detected.v1',1,$3,$4)`,
        [divergenceEventId, runId, input.ledgerAccountId, { eventId: divergenceEventId, runId, counts }],
      );
    }
    await client.query("COMMIT");
    metrics.reconciliationRuns.inc();
    metrics.reconciliationMatched.inc(counts.matched);
    metrics.reconciliationUnmatchedBank.inc(counts.missingInternal);
    metrics.reconciliationUnmatchedInternal.inc(counts.missingBank);
    metrics.reconciliationDivergences.inc(items.length - counts.matched);
    return { runId, replayed: false, items };
  } catch (error) { await client.query("ROLLBACK"); throw error; }
  finally { client.release(); }
}

function toMovement(row: any): Movement {
  return { id: row.id, endToEndId: row.end_to_end_id, providerTransactionId: row.provider_transaction_id,
    externalReference: row.external_reference, direction: row.direction, amountMinor: BigInt(row.amount_minor),
    currency: row.currency, occurredAt: new Date(row.occurred_at) };
}
function toMatchItem(row: any): MatchItem {
  return { transactionId: row.transaction_id, bankStatementEntryId: row.bank_statement_entry_id,
    matchKey: row.match_key, status: row.status, differenceMinor: BigInt(row.difference_minor), reason: row.reason };
}

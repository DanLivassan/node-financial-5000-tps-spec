import { randomUUID } from "node:crypto";
import pg from "pg";
import { accountPair, databaseUrl, targetUrl } from "./common.mjs";

const pair = await accountPair();
const key = `idempotency-load-${randomUUID()}`;
const body = { externalReference: key, ...pair, amountMinor: 1, currency: "BRL" };
const responses = await Promise.all(Array.from({ length: Math.max(100, Number.parseInt(process.env.IDEMPOTENCY_REQUESTS ?? "120", 10)) }, () =>
  fetch(`${targetUrl}/v1/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body: JSON.stringify(body) })));
const payloads = await Promise.all(responses.map((response) => response.json()));
const pool = new pg.Pool({ connectionString: databaseUrl });
const proof = await pool.query(`SELECT
 (SELECT count(*)::int FROM financial_transactions WHERE idempotency_key=$1) transactions,
 (SELECT count(*)::int FROM journal_entries j JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) journals,
 (SELECT count(*)::int FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) postings,
 (SELECT count(*)::int FROM outbox_events o JOIN financial_transactions t ON t.id=o.aggregate_id WHERE t.idempotency_key=$1) outbox`, [key]);
const conflict = await fetch(`${targetUrl}/v1/transactions`, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key },
  body: JSON.stringify({ ...body, amountMinor: 2 }) });
await pool.end();
const report = { requests: responses.length, statuses: [...new Set(responses.map((response) => response.status))],
  transactionIds: [...new Set(payloads.map((payload) => payload.id))], proof: proof.rows[0], conflictStatus: conflict.status };
console.log(JSON.stringify(report, null, 2));
if (report.transactionIds.length !== 1 || report.proof.transactions !== 1 || report.proof.journals !== 1 ||
  report.proof.postings !== 2 || report.proof.outbox !== 1 || conflict.status !== 409) process.exitCode = 1;

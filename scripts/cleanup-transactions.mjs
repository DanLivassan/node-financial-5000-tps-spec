import pg from "pg";
import { performance } from "node:perf_hooks";

if (process.env.ALLOW_DESTRUCTIVE_CLEANUP !== "true") throw new Error("Refusing cleanup: set ALLOW_DESTRUCTIVE_CLEANUP=true");
if (process.env.NODE_ENV === "production") throw new Error("Refusing cleanup in production");
const databaseUrl = process.env.DATABASE_URL ?? "postgres://financial:financial@localhost:5432/financial";
const parsed = new URL(databaseUrl);
const beforeArg = process.argv.find((arg) => arg.startsWith("--before="));
const before = beforeArg?.slice(9) ?? new Date().toISOString();
if (Number.isNaN(Date.parse(before))) throw new Error("--before must be an ISO timestamp");
const includeBalances = process.argv.includes("--include-balances");
const includeAccounts = process.argv.includes("--include-accounts");
console.log(`Target DB: host=${parsed.hostname} database=${parsed.pathname.slice(1)} before=${before}`);
const pool = new pg.Pool({ connectionString: databaseUrl, max: 1 });
const client = await pool.connect(); const started = performance.now(); const counts = {};
async function remove(name, sql, params = [before]) { const result = await client.query(sql, params); counts[name] = result.rowCount ?? 0; }
try {
  await client.query("BEGIN");
  await remove("reconciliation_items", "DELETE FROM reconciliation_items WHERE reconciliation_run_id IN (SELECT id FROM reconciliation_runs WHERE started_at < $1)");
  await remove("reconciliation_runs", "DELETE FROM reconciliation_runs WHERE started_at < $1");
  await remove("bank_statement_entries", "DELETE FROM bank_statement_entries WHERE imported_at < $1");
  await remove("processed_financial_events", "DELETE FROM processed_financial_events WHERE created_at < $1");
  await remove("consumed_events", "DELETE FROM consumed_events WHERE processed_at < $1");
  await remove("redis_lab_deliveries", "DELETE FROM redis_lab_deliveries WHERE delivered_at < $1");
  await remove("outbox_events", "DELETE FROM outbox_events WHERE created_at < $1");
  await client.query("ALTER TABLE ledger_postings DISABLE TRIGGER ledger_postings_immutable");
  await remove("ledger_postings", "DELETE FROM ledger_postings WHERE created_at < $1");
  await client.query("ALTER TABLE journal_entries DISABLE TRIGGER journal_entries_immutable");
  await remove("journal_entries", "DELETE FROM journal_entries WHERE created_at < $1");
  await remove("financial_transactions", "DELETE FROM financial_transactions WHERE created_at < $1");
  await client.query("ALTER TABLE ledger_postings ENABLE TRIGGER ledger_postings_immutable");
  await client.query("ALTER TABLE journal_entries ENABLE TRIGGER journal_entries_immutable");
  if (includeBalances || includeAccounts) await remove("account_balances", "DELETE FROM account_balances WHERE updated_at < $1");
  if (includeAccounts) {
    await remove("balance_audit_events", "DELETE FROM balance_audit_events WHERE created_at < $1");
    await remove("ledger_accounts", "DELETE FROM ledger_accounts WHERE created_at < $1");
  }
  await client.query("COMMIT");
  console.log(JSON.stringify({ removed: counts, elapsedMs: Math.round(performance.now() - started) }, null, 2));
} catch (error) { await client.query("ROLLBACK"); throw error; }
finally { client.release(); await pool.end(); }

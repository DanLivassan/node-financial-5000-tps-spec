import { pool } from "../packages/database/src/pool.js";
import { createFinancialTransaction } from "../packages/ledger/src/transaction-service.js";

const accounts = [
  ["EQUITY:OPENING", "equity", true],
  ["CUSTOMER:A", "liability", false],
  ["CUSTOMER:B", "liability", false],
  ["BANK:SETTLEMENT", "asset", true],
  ["CLEARING:SUSPENSE", "asset", true],
  ["REVENUE:FEES", "revenue", true],
  ["RECONCILIATION:ADJUSTMENT", "expense", true],
] as const;
for (const [code, type, allowNegative] of accounts) {
  await pool.query(
    `INSERT INTO ledger_accounts(code,account_type,currency,allow_negative)
     VALUES ($1,$2,'BRL',$3) ON CONFLICT (code) DO NOTHING`, [code, type, allowNegative],
  );
}
const selected = await pool.query<{ id: string; code: string }>("SELECT id,code FROM ledger_accounts WHERE code=ANY($1)", [accounts.map(([code]) => code)]);
const ids = new Map(selected.rows.map((row) => [row.code, row.id]));
for (const code of ["CUSTOMER:A", "CUSTOMER:B"] as const) {
  await createFinancialTransaction(pool, `seed-opening-${code}`, {
    externalReference: `seed-opening-${code}`,
    sourceAccountId: ids.get("EQUITY:OPENING")!,
    destinationAccountId: ids.get(code)!,
    amountMinor: 100_000_000,
    currency: "BRL",
  });
}
console.log(Object.fromEntries(ids));
await pool.end();

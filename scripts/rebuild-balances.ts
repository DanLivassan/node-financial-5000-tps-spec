import { pool } from "../packages/database/src/pool.js";
import { auditBalances, recordAndRepairBalances } from "../packages/ledger/src/balance-rebuild.js";

const accountArg = process.argv.find((arg) => arg.startsWith("--account="));
const accountId = accountArg?.slice("--account=".length);
const differences = await auditBalances(pool, accountId);
console.log(JSON.stringify(differences, (_key, value) => typeof value === "bigint" ? value.toString() : value, 2));
if (process.argv.includes("--apply")) {
  console.log(`Repaired ${await recordAndRepairBalances(pool, accountId)} balance projection(s); audit rows were recorded.`);
} else {
  console.log("Dry run only. Pass --apply to repair after recording audit events.");
}
await pool.end();

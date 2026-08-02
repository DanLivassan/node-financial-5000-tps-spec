import { pool } from "../packages/database/src/pool.js";
import { auditBalances } from "../packages/ledger/src/balance-rebuild.js";

const accountArg = process.argv.find((arg) => arg.startsWith("--account="));
const rows = await auditBalances(pool, accountArg?.slice("--account=".length));
console.log(JSON.stringify(rows, (_key, value) => typeof value === "bigint" ? value.toString() : value, 2));
process.exitCode = rows.length === 0 ? 0 : 2;
await pool.end();

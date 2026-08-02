import { readFile } from "node:fs/promises";
import { pool } from "../../packages/database/src/pool.js";
import { runReconciliation } from "../../packages/ledger/src/reconciliation.js";

const file = process.argv[2];
if (!file) throw new Error("usage: <reconciliation-input.json>");
const result = await runReconciliation(pool, JSON.parse(await readFile(file, "utf8")));
console.log(JSON.stringify(result, (_key, value) => typeof value === "bigint" ? value.toString() : value, 2));
await pool.end();

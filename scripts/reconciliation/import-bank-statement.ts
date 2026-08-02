import { readFile } from "node:fs/promises";
import { pool } from "../../packages/database/src/pool.js";
import { importBankStatement } from "../../packages/ledger/src/reconciliation.js";

const [file, provider, bankAccountId] = process.argv.slice(2);
if (!file || !provider || !bankAccountId) throw new Error("usage: <file.json> <provider> <bankAccountId>");
const entries = JSON.parse(await readFile(file, "utf8"));
console.log(await importBankStatement(pool, provider, bankAccountId, entries));
await pool.end();

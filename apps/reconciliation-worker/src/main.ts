import Fastify from "fastify";
import { z } from "zod";
import { pool } from "../../../packages/database/src/pool.js";
import { importBankStatement, runReconciliation } from "../../../packages/ledger/src/reconciliation.js";
import { createCompensatingEntry } from "../../../packages/ledger/src/reversal-service.js";

const app = Fastify({ logger: true, bodyLimit: 2 * 1024 * 1024 });
app.post("/v1/statements/import", async (request) => {
  const body = z.object({ provider: z.string().min(1), bankAccountId: z.string().min(1), entries: z.array(z.any()).max(10_000) }).parse(request.body);
  return importBankStatement(pool, body.provider, body.bankAccountId, body.entries);
});
app.post("/v1/reconciliation-runs", async (request) => runReconciliation(pool, z.any().parse(request.body)));
app.post<{ Params: { transactionId: string } }>("/v1/transactions/:transactionId/reverse", async (request) =>
  createCompensatingEntry(pool, request.params.transactionId));
const port = Number.parseInt(process.env.RECONCILIATION_PORT ?? "3003", 10);
const shutdown = async () => { await app.close(); await pool.end(); };
process.on("SIGINT", shutdown); process.on("SIGTERM", shutdown);
await app.listen({ host: "0.0.0.0", port });

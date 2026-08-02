import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildServer } from "../apps/transaction-api/src/server.js";
import { pool } from "../packages/database/src/pool.js";

const source = randomUUID();
const destination = randomUUID();
const app = buildServer(pool);

beforeAll(async () => {
  await pool.query(
    `INSERT INTO ledger_accounts(id,code,account_type,currency) VALUES
     ($1,$2,'liability','BRL'),($3,$4,'liability','BRL')`,
    [source, `api-source-${source}`, destination, `api-dest-${destination}`],
  );
});
afterAll(async () => { await app.close(); await pool.end(); });

describe("transaction API", () => {
  const payload = { externalReference: `api-${randomUUID()}`, sourceAccountId: source,
    destinationAccountId: destination, amountMinor: 100, currency: "BRL" };

  it("requires the idempotency header", async () => {
    const response = await app.inject({ method: "POST", url: "/v1/transactions", payload });
    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ error: "INVALID_IDEMPOTENCY_KEY" });
  });

  it("returns stable insufficient funds without partial persistence", async () => {
    const key = `insufficient-${randomUUID()}`;
    const response = await app.inject({ method: "POST", url: "/v1/transactions", headers: { "idempotency-key": key }, payload });
    expect(response.statusCode).toBe(422);
    expect(response.json()).toMatchObject({ error: "INSUFFICIENT_FUNDS" });
    const count = await pool.query<{ count: bigint }>("SELECT count(*) FROM financial_transactions WHERE idempotency_key=$1", [key]);
    expect(count.rows[0]!.count).toBe(0n);
  });

  it("exposes health and Prometheus metrics", async () => {
    expect((await app.inject({ method: "GET", url: "/health" })).statusCode).toBe(200);
    const response = await app.inject({ method: "GET", url: "/metrics" });
    expect(response.statusCode).toBe(200);
    expect(response.body).toContain("http_requests_total");
    expect(response.body).toContain("outbox_oldest_pending_age_seconds");
  });
});

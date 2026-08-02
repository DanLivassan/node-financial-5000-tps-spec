import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { buildServer } from "../apps/transaction-api/src/server.js";
import { pool } from "../packages/database/src/pool.js";
import { auditBalances } from "../packages/ledger/src/balance-rebuild.js";
import { createFinancialTransactionBatch } from "../packages/ledger/src/transaction-batch-service.js";
import { createFinancialTransaction } from "../packages/ledger/src/transaction-service.js";

const fundingId = randomUUID();
const sourceA = randomUUID();
const sourceB = randomUUID();
const destination = randomUUID();
const app = buildServer(pool);

beforeAll(async () => {
  await pool.query(
    `INSERT INTO ledger_accounts(id,code,account_type,currency,allow_negative) VALUES
     ($1,$2,'equity','BRL',true),($3,$4,'liability','BRL',false),
     ($5,$6,'liability','BRL',false),($7,$8,'liability','BRL',false)`,
    [fundingId, `batch-funding-${fundingId}`, sourceA, `batch-a-${sourceA}`,
      sourceB, `batch-b-${sourceB}`, destination, `batch-dest-${destination}`],
  );
  await createFinancialTransaction(pool, `batch-fund-a-${sourceA}`, {
    externalReference: `batch-fund-a-${sourceA}`, sourceAccountId: fundingId, destinationAccountId: sourceA,
    amountMinor: 1_000, currency: "BRL",
  });
  await createFinancialTransaction(pool, `batch-fund-b-${sourceB}`, {
    externalReference: `batch-fund-b-${sourceB}`, sourceAccountId: fundingId, destinationAccountId: sourceB,
    amountMinor: 50, currency: "BRL",
  });
});

afterAll(async () => { await app.close(); await pool.end(); }, 30_000);

function command(key: string, sourceAccountId: string, amountMinor: number) {
  return {
    idempotencyKey: key,
    request: {
      externalReference: key,
      sourceAccountId,
      destinationAccountId: destination,
      amountMinor,
      currency: "BRL",
    },
  };
}

describe("financial transaction microbatch", () => {
  it("keeps each HTTP request open and maps the committed result to the correct client", async () => {
    const inputs = [31, 32, 33].map((amountMinor) => ({
      key: `batch-http-${amountMinor}-${randomUUID()}`,
      payload: {
        externalReference: `batch-http-ref-${amountMinor}-${randomUUID()}`,
        sourceAccountId: sourceA,
        destinationAccountId: destination,
        amountMinor,
        currency: "BRL",
      },
    }));
    const responses = await Promise.all(inputs.map((input) => app.inject({
      method: "POST",
      url: "/v1/transactions",
      headers: { "idempotency-key": input.key },
      payload: input.payload,
    })));
    expect(responses.map((response) => response.statusCode)).toEqual([201, 201, 201]);
    expect(responses.map((response) => response.json().externalReference))
      .toEqual(inputs.map((input) => input.payload.externalReference));
    expect(responses.map((response) => response.headers["idempotency-replayed"]))
      .toEqual(["false", "false", "false"]);
    const count = await pool.query<{ count: bigint }>(
      "SELECT count(*) FROM financial_transactions WHERE idempotency_key=ANY($1::text[])",
      [inputs.map((input) => input.key)],
    );
    expect(count.rows[0]!.count).toBe(3n);
  });

  it("commits valid items together and isolates insufficient funds", async () => {
    const keys = [`batch-valid-a-${randomUUID()}`, `batch-invalid-${randomUUID()}`, `batch-valid-b-${randomUUID()}`];
    const outcomes = await createFinancialTransactionBatch(pool, [
      command(keys[0]!, sourceA, 100),
      command(keys[1]!, sourceB, 100),
      command(keys[2]!, sourceA, 200),
    ]);
    expect(outcomes.map((outcome) => outcome.ok)).toEqual([true, false, true]);
    expect(outcomes[1]).toMatchObject({ ok: false, error: { code: "INSUFFICIENT_FUNDS", httpStatus: 422 } });

    const proof = await pool.query<{
      transactions: bigint; journals: bigint; postings: bigint; outbox: bigint;
      source_a: bigint; source_b: bigint; destination: bigint;
    }>(
      `SELECT
        (SELECT count(*) FROM financial_transactions WHERE idempotency_key=ANY($1::text[])) transactions,
        (SELECT count(*) FROM journal_entries j JOIN financial_transactions t ON t.id=j.transaction_id
          WHERE t.idempotency_key=ANY($1::text[])) journals,
        (SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id
          JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=ANY($1::text[])) postings,
        (SELECT count(*) FROM outbox_events o JOIN financial_transactions t ON t.id=o.aggregate_id
          WHERE t.idempotency_key=ANY($1::text[])) outbox,
        (SELECT available_minor FROM account_balances WHERE account_id=$2 AND currency='BRL') source_a,
        (SELECT available_minor FROM account_balances WHERE account_id=$3 AND currency='BRL') source_b,
        (SELECT available_minor FROM account_balances WHERE account_id=$4 AND currency='BRL') destination`,
      [keys, sourceA, sourceB, destination],
    );
    expect(proof.rows[0]).toEqual({
      transactions: 2n, journals: 2n, postings: 4n, outbox: 2n,
      source_a: 604n, source_b: 50n, destination: 396n,
    });
    expect(await auditBalances(pool, sourceA)).toEqual([]);
    expect(await auditBalances(pool, sourceB)).toEqual([]);
    expect(await auditBalances(pool, destination)).toEqual([]);
  });

  it("deduplicates concurrent batches into one financial effect", async () => {
    const key = `batch-race-${randomUUID()}`;
    const input = command(key, sourceA, 10);
    const [left, right] = await Promise.all([
      createFinancialTransactionBatch(pool, [input]),
      createFinancialTransactionBatch(pool, [input]),
    ]);
    const results = [left[0], right[0]];
    expect(results.every((outcome) => outcome?.ok)).toBe(true);
    expect(results.filter((outcome) => outcome?.ok && !outcome.result.replayed)).toHaveLength(1);
    expect(results.filter((outcome) => outcome?.ok && outcome.result.replayed)).toHaveLength(1);
    const ids = results.flatMap((outcome) => outcome?.ok ? [outcome.result.body.id] : []);
    expect(new Set(ids).size).toBe(1);

    const proof = await pool.query<{ transactions: bigint; journals: bigint; postings: bigint; outbox: bigint }>(
      `SELECT
        (SELECT count(*) FROM financial_transactions WHERE idempotency_key=$1) transactions,
        (SELECT count(*) FROM journal_entries j JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) journals,
        (SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id
          JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) postings,
        (SELECT count(*) FROM outbox_events o JOIN financial_transactions t ON t.id=o.aggregate_id WHERE t.idempotency_key=$1) outbox`,
      [key],
    );
    expect(proof.rows[0]).toEqual({ transactions: 1n, journals: 1n, postings: 2n, outbox: 1n });
  });

  it("rolls back every financial effect when the batch commit path fails", async () => {
    const keys = [`batch-rollback-a-${randomUUID()}`, `batch-rollback-b-${randomUUID()}`];
    const before = await pool.query<{ value: bigint }>(
      "SELECT available_minor value FROM account_balances WHERE account_id=$1 AND currency='BRL'", [sourceA],
    );
    await expect(createFinancialTransactionBatch(pool, [
      command(keys[0]!, sourceA, 10), command(keys[1]!, sourceA, 20),
    ], { beforeCommit: () => { throw new Error("forced batch rollback"); } })).rejects.toThrow("forced batch rollback");
    const proof = await pool.query<{ transactions: bigint; journals: bigint; postings: bigint; outbox: bigint; balance: bigint }>(
      `SELECT
        (SELECT count(*) FROM financial_transactions WHERE idempotency_key=ANY($1::text[])) transactions,
        (SELECT count(*) FROM journal_entries j JOIN financial_transactions t ON t.id=j.transaction_id
          WHERE t.idempotency_key=ANY($1::text[])) journals,
        (SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id
          JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=ANY($1::text[])) postings,
        (SELECT count(*) FROM outbox_events o JOIN financial_transactions t ON t.id=o.aggregate_id
          WHERE t.idempotency_key=ANY($1::text[])) outbox,
        (SELECT available_minor FROM account_balances WHERE account_id=$2 AND currency='BRL') balance`,
      [keys, sourceA],
    );
    expect(proof.rows[0]).toEqual({ transactions: 0n, journals: 0n, postings: 0n, outbox: 0n, balance: before.rows[0]!.value });
  });
});

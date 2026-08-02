import { randomUUID } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { pool } from "../packages/database/src/pool.js";
import { createFinancialTransaction } from "../packages/ledger/src/transaction-service.js";
import { auditBalances } from "../packages/ledger/src/balance-rebuild.js";

const fundingId = randomUUID();
const sourceId = randomUUID();
const destinationId = randomUUID();

beforeAll(async () => {
  await pool.query(
    `INSERT INTO ledger_accounts(id,code,account_type,currency,allow_negative) VALUES
     ($1,$2,'equity','BRL',true),($3,$4,'liability','BRL',false),($5,$6,'liability','BRL',false)`,
    [fundingId, `test-funding-${fundingId}`, sourceId, `test-source-${sourceId}`, destinationId, `test-dest-${destinationId}`],
  );
  await createFinancialTransaction(pool, `fund-${sourceId}`, {
    externalReference: `fund-${sourceId}`,
    sourceAccountId: fundingId,
    destinationAccountId: sourceId,
    amountMinor: 1_000_000,
    currency: "BRL",
  });
});

afterAll(async () => { await pool.end(); }, 30_000);

describe("atomic financial transaction", () => {
  it("deduplicates 120 concurrent requests into one financial effect", async () => {
    const key = `concurrent-${randomUUID()}`;
    const request = {
      externalReference: `order-${randomUUID()}`,
      sourceAccountId: sourceId,
      destinationAccountId: destinationId,
      amountMinor: 100,
      currency: "BRL",
    };
    const results = await Promise.all(Array.from({ length: 120 }, () =>
      createFinancialTransaction(pool, key, request),
    ));
    expect(new Set(results.map((result) => result.body.id)).size).toBe(1);
    expect(results.filter((result) => !result.replayed)).toHaveLength(1);

    const proof = await pool.query<{
      transactions: bigint; journals: bigint; postings: bigint; outbox: bigint;
      source_balance: bigint; destination_balance: bigint;
    }>(
      `SELECT
        (SELECT count(*) FROM financial_transactions WHERE idempotency_key=$1) transactions,
        (SELECT count(*) FROM journal_entries j JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) journals,
        (SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id
          JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) postings,
        (SELECT count(*) FROM outbox_events o JOIN financial_transactions t ON t.id=o.aggregate_id WHERE t.idempotency_key=$1) outbox,
        (SELECT available_minor FROM account_balances WHERE account_id=$2 AND currency='BRL') source_balance,
        (SELECT available_minor FROM account_balances WHERE account_id=$3 AND currency='BRL') destination_balance`,
      [key, sourceId, destinationId],
    );
    expect(proof.rows[0]).toEqual({
      transactions: 1n,
      journals: 1n,
      postings: 2n,
      outbox: 1n,
      source_balance: 999_900n,
      destination_balance: 100n,
    });

    await expect(createFinancialTransaction(pool, key, { ...request, amountMinor: 101 }))
      .rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT", httpStatus: 409 });
  });

  it("rolls back transaction, journal, postings, balances and outbox together", async () => {
    const key = `rollback-${randomUUID()}`;
    const before = await pool.query<{ value: bigint }>(
      "SELECT available_minor value FROM account_balances WHERE account_id=$1 AND currency='BRL'", [sourceId],
    );
    await expect(createFinancialTransaction(pool, key, {
      externalReference: key,
      sourceAccountId: sourceId,
      destinationAccountId: destinationId,
      amountMinor: 50,
      currency: "BRL",
    }, { beforeCommit: () => { throw new Error("forced rollback"); } })).rejects.toThrow("forced rollback");
    const proof = await pool.query<{ transactions: bigint; journals: bigint; postings: bigint; outbox: bigint; balance: bigint }>(
      `SELECT
        (SELECT count(*) FROM financial_transactions WHERE idempotency_key=$1) transactions,
        (SELECT count(*) FROM journal_entries j JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) journals,
        (SELECT count(*) FROM ledger_postings p JOIN journal_entries j ON j.id=p.journal_entry_id
          JOIN financial_transactions t ON t.id=j.transaction_id WHERE t.idempotency_key=$1) postings,
        (SELECT count(*) FROM outbox_events WHERE aggregate_id IN (SELECT id FROM financial_transactions WHERE idempotency_key=$1)) outbox,
        (SELECT available_minor FROM account_balances WHERE account_id=$2 AND currency='BRL') balance`, [key, sourceId],
    );
    expect(proof.rows[0]).toEqual({ transactions: 0n, journals: 0n, postings: 0n, outbox: 0n, balance: before.rows[0]!.value });
  });

  it("keeps a hot destination balance equal to ledger under contention", async () => {
    const before = await pool.query<{ value: bigint }>(
      "SELECT available_minor value FROM account_balances WHERE account_id=$1 AND currency='BRL'", [destinationId],
    );
    const operations = await Promise.allSettled(Array.from({ length: 5 }, (_, index) => createFinancialTransaction(pool, `hot-${randomUUID()}-${index}`, {
      externalReference: `hot-${randomUUID()}-${index}`, sourceAccountId: sourceId, destinationAccountId: destinationId,
      amountMinor: 10, currency: "BRL",
    })));
    expect(operations.filter((operation) => operation.status === "rejected").map((operation) => ({
      message: String(operation.reason), code: (operation.reason as { code?: string }).code,
      detail: (operation.reason as { detail?: string }).detail,
    }))).toEqual([]);
    const after = await pool.query<{ value: bigint }>(
      "SELECT available_minor value FROM account_balances WHERE account_id=$1 AND currency='BRL'", [destinationId],
    );
    expect(after.rows[0]!.value - before.rows[0]!.value).toBe(50n);
    expect(await auditBalances(pool, destinationId)).toEqual([]);
    expect(await auditBalances(pool, sourceId)).toEqual([]);
  });
});

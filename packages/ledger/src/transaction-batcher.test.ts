import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import type { TransactionRequest } from "../../contracts/src/transactions.js";
import type { BatchTransactionCommand, BatchTransactionOutcome } from "./transaction-batch-service.js";
import { TransactionBatcher } from "./transaction-batcher.js";

function request(amountMinor = 10): TransactionRequest {
  return {
    externalReference: randomUUID(),
    sourceAccountId: randomUUID(),
    destinationAccountId: randomUUID(),
    amountMinor,
    currency: "BRL",
  };
}

function accepted(command: BatchTransactionCommand): BatchTransactionOutcome {
  return { ok: true, result: { statusCode: 201, replayed: false, body: {
    id: randomUUID(),
    journalEntryId: randomUUID(),
    externalReference: command.request.externalReference,
    status: "accepted",
    amountMinor: command.request.amountMinor,
    currency: command.request.currency,
    createdAt: new Date().toISOString(),
  } } };
}

const options = { maxBatchSize: 3, maxWaitMs: 10, maxConcurrentBatches: 1, maxQueueSize: 10 };

describe("TransactionBatcher", () => {
  it("maps each response to its request and waits for the batch executor", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const executed: BatchTransactionCommand[][] = [];
    const batcher = new TransactionBatcher(async (commands) => {
      executed.push(commands);
      await gate;
      return commands.map(accepted);
    }, options);
    const inputs = [request(11), request(12), request(13)];
    let completed = false;
    const resultsPromise = Promise.all(inputs.map((input, index) =>
      batcher.submit(`key-${index}`, input))).then((results) => { completed = true; return results; });

    await new Promise((resolve) => setImmediate(resolve));
    expect(executed).toHaveLength(1);
    expect(executed[0]).toHaveLength(3);
    expect(completed).toBe(false);
    release();
    const results = await resultsPromise;
    expect(results.map((result) => result.body.amountMinor)).toEqual([11, 12, 13]);
    await batcher.close();
  });

  it("coalesces an in-flight duplicate and marks the follower as replay", async () => {
    let calls = 0;
    const batcher = new TransactionBatcher(async (commands) => {
      calls += 1;
      return commands.map(accepted);
    }, { ...options, maxBatchSize: 2 });
    const payload = request();
    const first = batcher.submit("same-key", payload);
    const duplicate = batcher.submit("same-key", payload);
    await batcher.close();
    const [winner, replay] = await Promise.all([first, duplicate]);
    expect(calls).toBe(1);
    expect(winner.replayed).toBe(false);
    expect(replay.replayed).toBe(true);
    expect(replay.body.id).toBe(winner.body.id);
  });

  it("rejects a different payload while the key is in flight", async () => {
    const batcher = new TransactionBatcher(async (commands) => commands.map(accepted), options);
    const payload = request(10);
    const first = batcher.submit("conflicting-key", payload);
    await expect(batcher.submit("conflicting-key", { ...payload, amountMinor: 11 }))
      .rejects.toMatchObject({ code: "IDEMPOTENCY_CONFLICT", httpStatus: 409 });
    await batcher.close();
    await expect(first).resolves.toMatchObject({ statusCode: 201 });
  });

  it("applies bounded backpressure across queued and executing commands", async () => {
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const batcher = new TransactionBatcher(async (commands) => {
      await gate;
      return commands.map(accepted);
    }, { ...options, maxBatchSize: 1, maxQueueSize: 1 });
    const first = batcher.submit("queue-1", request());
    await expect(batcher.submit("queue-2", request()))
      .rejects.toMatchObject({ code: "BATCH_QUEUE_FULL", httpStatus: 503 });
    release();
    await first;
    await batcher.close();
  });

  it("keeps per-item failures isolated", async () => {
    const expected = new Error("individual failure");
    const batcher = new TransactionBatcher(async (commands) => commands.map((command, index) =>
      index === 1 ? { ok: false, error: expected } : accepted(command)), options);
    const results = await Promise.allSettled([
      batcher.submit("isolated-1", request()),
      batcher.submit("isolated-2", request()),
      batcher.submit("isolated-3", request()),
    ]);
    expect(results.map((result) => result.status)).toEqual(["fulfilled", "rejected", "fulfilled"]);
    await batcher.close();
  });
});

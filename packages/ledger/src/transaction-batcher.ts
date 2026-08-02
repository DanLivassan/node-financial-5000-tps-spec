import type { TransactionRequest } from "../../contracts/src/transactions.js";
import { metrics } from "../../observability/src/metrics.js";
import { requestHash } from "./idempotency.js";
import type { BatchTransactionCommand, BatchTransactionOutcome } from "./transaction-batch-service.js";
import { DomainError, type TransactionResult } from "./transaction-service.js";

interface QueuedCommand {
  command: BatchTransactionCommand;
  enqueuedAt: bigint;
  resolve: (result: TransactionResult) => void;
  reject: (error: unknown) => void;
}

interface InFlightRequest {
  hash: string;
  promise: Promise<TransactionResult>;
}

export interface TransactionBatcherOptions {
  maxBatchSize: number;
  maxWaitMs: number;
  maxConcurrentBatches: number;
  maxQueueSize: number;
}

export type TransactionBatchExecutor =
  (commands: BatchTransactionCommand[]) => Promise<BatchTransactionOutcome[]>;

function positiveInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
}

export class TransactionBatcher {
  private readonly queue: QueuedCommand[] = [];
  private readonly inFlightByKey = new Map<string, InFlightRequest>();
  private readonly idleWaiters: Array<() => void> = [];
  private timer: NodeJS.Timeout | undefined;
  private activeBatches = 0;
  private closed = false;

  constructor(
    private readonly execute: TransactionBatchExecutor,
    private readonly options: TransactionBatcherOptions,
  ) {
    positiveInteger("maxBatchSize", options.maxBatchSize);
    positiveInteger("maxWaitMs", options.maxWaitMs);
    positiveInteger("maxConcurrentBatches", options.maxConcurrentBatches);
    positiveInteger("maxQueueSize", options.maxQueueSize);
  }

  submit(idempotencyKey: string, request: TransactionRequest, traceparent?: string): Promise<TransactionResult> {
    if (this.closed) return Promise.reject(new DomainError("BATCHER_CLOSED", 503, "transaction processor is shutting down"));
    if (!idempotencyKey || idempotencyKey.length > 128) {
      return Promise.reject(new DomainError("INVALID_IDEMPOTENCY_KEY", 400,
        "Idempotency-Key is required and must have at most 128 characters"));
    }
    const hash = requestHash(request);
    const inFlight = this.inFlightByKey.get(idempotencyKey);
    if (inFlight) {
      if (inFlight.hash !== hash) {
        metrics.idempotencyConflicts.inc();
        return Promise.reject(new DomainError("IDEMPOTENCY_CONFLICT", 409,
          "Idempotency-Key is already processing another payload"));
      }
      return inFlight.promise.then((result) => ({ ...result, replayed: true }));
    }
    if (this.inFlightByKey.size >= this.options.maxQueueSize) {
      metrics.transactionBatchRejected.inc();
      return Promise.reject(new DomainError("BATCH_QUEUE_FULL", 503, "transaction queue is full"));
    }

    let resolve!: (result: TransactionResult) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<TransactionResult>((resolvePromise, rejectPromise) => {
      resolve = resolvePromise;
      reject = rejectPromise;
    });
    this.inFlightByKey.set(idempotencyKey, { hash, promise });
    this.queue.push({
      command: { idempotencyKey, request, ...(traceparent ? { traceparent } : {}) },
      enqueuedAt: process.hrtime.bigint(),
      resolve,
      reject,
    });
    metrics.transactionBatchQueued.set(this.queue.length);
    this.pump(false);
    return promise;
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    this.pump(true);
    if (this.queue.length === 0 && this.activeBatches === 0) return;
    await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  private pump(forcePartial: boolean): void {
    while (this.activeBatches < this.options.maxConcurrentBatches
      && this.queue.length > 0
      && (forcePartial || this.queue.length >= this.options.maxBatchSize)) {
      const batch = this.queue.splice(0, this.options.maxBatchSize);
      metrics.transactionBatchQueued.set(this.queue.length);
      this.dispatch(batch);
    }
    if (this.queue.length > 0 && this.activeBatches < this.options.maxConcurrentBatches && !this.timer) {
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.pump(true);
      }, this.options.maxWaitMs);
      this.timer.unref();
    }
    this.resolveIdleIfNeeded();
  }

  private dispatch(batch: QueuedCommand[]): void {
    this.activeBatches += 1;
    metrics.transactionBatchActive.set(this.activeBatches);
    metrics.transactionBatchSize.observe(batch.length);
    const startedAt = process.hrtime.bigint();
    for (const item of batch) {
      metrics.transactionBatchQueueWait.observe(Number(startedAt - item.enqueuedAt) / 1e9);
    }
    void this.execute(batch.map((item) => item.command))
      .then((outcomes) => {
        if (outcomes.length !== batch.length) throw new Error("batch executor returned an invalid outcome count");
        for (const [index, item] of batch.entries()) {
          const outcome = outcomes[index]!;
          if (outcome.ok) item.resolve(outcome.result);
          else item.reject(outcome.error);
        }
      })
      .catch((error: unknown) => {
        for (const item of batch) item.reject(error);
      })
      .finally(() => {
        metrics.transactionBatchDuration.observe(Number(process.hrtime.bigint() - startedAt) / 1e9);
        for (const item of batch) this.inFlightByKey.delete(item.command.idempotencyKey);
        this.activeBatches -= 1;
        metrics.transactionBatchActive.set(this.activeBatches);
        this.pump(this.closed || this.queue.length >= this.options.maxBatchSize);
      });
  }

  private resolveIdleIfNeeded(): void {
    if (this.queue.length !== 0 || this.activeBatches !== 0) return;
    for (const resolve of this.idleWaiters.splice(0)) resolve();
  }
}

import { createServer, type Server } from "node:http";
import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from "prom-client";
import type pg from "pg";

export const registry = new Registry();
let defaultsStarted = false;
export function initializeDefaultMetrics(): void {
  if (!defaultsStarted) { collectDefaultMetrics({ register: registry, prefix: "" }); defaultsStarted = true; }
}

const counter = (name: string, help: string, labelNames: string[] = []) =>
  new Counter({ name, help, labelNames, registers: [registry] });
export const metrics = {
  httpRequests: counter("http_requests_total", "HTTP requests", ["method", "route", "status_code"]),
  httpDuration: new Histogram({ name: "http_request_duration_seconds", help: "HTTP request duration", labelNames: ["method", "route"],
    buckets: [0.001,0.003,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5], registers: [registry] }),
  transactionsCreated: counter("financial_transactions_created_total", "Accepted financial transactions"),
  idempotencyReplays: counter("idempotency_replays_total", "HTTP idempotency replays"),
  idempotencyConflicts: counter("idempotency_conflicts_total", "HTTP idempotency conflicts"),
  ledgerEntries: counter("ledger_entries_created_total", "Posted journal entries"),
  ledgerUnbalanced: counter("ledger_unbalanced_attempts_total", "Rejected unbalanced journal attempts"),
  balanceRetries: counter("balance_update_retries_total", "Deadlock or serialization retries", ["sqlstate"]),
  balanceDiscrepancies: counter("balance_projection_discrepancies_total", "Balance audit discrepancies"),
  outboxPending: new Gauge({ name: "outbox_pending_total", help: "Pending/failed outbox rows", registers: [registry] }),
  outboxOldestAge: new Gauge({ name: "outbox_oldest_pending_age_seconds", help: "Age of oldest publishable outbox row", registers: [registry] }),
  outboxPublished: counter("outbox_published_total", "Outbox rows acknowledged by Kafka"),
  outboxFailures: counter("outbox_publish_failures_total", "Outbox Kafka publication failures"),
  reconciliationRuns: counter("reconciliation_runs_total", "Completed reconciliation runs"),
  reconciliationMatched: counter("reconciliation_matched_total", "Matched reconciliation movements"),
  reconciliationDivergences: counter("reconciliation_divergences_total", "Reconciliation divergences"),
  reconciliationUnmatchedBank: counter("reconciliation_unmatched_bank_total", "Internal movements absent at bank"),
  reconciliationUnmatchedInternal: counter("reconciliation_unmatched_internal_total", "Bank movements absent internally"),
  kafkaProducerErrors: counter("kafka_producer_errors_total", "Kafka producer errors"),
  consumerDuplicates: counter("consumer_duplicate_events_total", "Kafka events ignored as duplicates", ["consumer"]),
  pgPoolTotal: new Gauge({ name: "postgres_pool_total", help: "PostgreSQL pool clients", registers: [registry] }),
  pgPoolIdle: new Gauge({ name: "postgres_pool_idle", help: "PostgreSQL idle pool clients", registers: [registry] }),
  pgPoolWaiting: new Gauge({ name: "postgres_pool_waiting", help: "PostgreSQL pool waiters", registers: [registry] }),
  transactionBatchSize: new Histogram({ name: "transaction_batch_size", help: "Financial commands per PostgreSQL batch",
    buckets: [1,2,4,8,16,32,64,128], registers: [registry] }),
  transactionBatchQueueWait: new Histogram({ name: "transaction_batch_queue_wait_seconds", help: "Time a financial command waits for dispatch",
    buckets: [0.001,0.002,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1], registers: [registry] }),
  transactionBatchDuration: new Histogram({ name: "transaction_batch_duration_seconds", help: "PostgreSQL financial batch execution duration",
    buckets: [0.001,0.003,0.005,0.01,0.025,0.05,0.1,0.25,0.5,1,2.5], registers: [registry] }),
  transactionBatchActive: new Gauge({ name: "transaction_batch_active", help: "Financial batches executing", registers: [registry] }),
  transactionBatchQueued: new Gauge({ name: "transaction_batch_queued", help: "Financial commands waiting for dispatch", registers: [registry] }),
  transactionBatchRejected: counter("transaction_batch_rejected_total", "Financial commands rejected by queue backpressure"),
};

export async function updateDatabaseMetrics(db: pg.Pool): Promise<void> {
  metrics.pgPoolTotal.set(db.totalCount); metrics.pgPoolIdle.set(db.idleCount); metrics.pgPoolWaiting.set(db.waitingCount);
  const result = await db.query<{ pending: bigint; age: number | null }>(
    `SELECT count(*)::bigint pending,
      extract(epoch from now()-min(created_at))::float8 age
     FROM outbox_events WHERE status IN ('pending','failed')`,
  );
  metrics.outboxPending.set(Number(result.rows[0]?.pending ?? 0n));
  metrics.outboxOldestAge.set(Math.max(0, result.rows[0]?.age ?? 0));
}

export function startMetricsServer(db: pg.Pool, port: number): Server {
  initializeDefaultMetrics();
  return createServer(async (request, response) => {
    if (request.url !== "/metrics") { response.writeHead(404).end(); return; }
    try {
      await updateDatabaseMetrics(db);
      response.writeHead(200, { "content-type": registry.contentType }); response.end(await registry.metrics());
    } catch (error) { response.writeHead(500).end(String(error)); }
  }).listen(port);
}

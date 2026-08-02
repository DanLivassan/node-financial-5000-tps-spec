function integer(name: string, fallback: number): number {
  const parsed = Number.parseInt(process.env[name] ?? String(fallback), 10);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`${name} must be a positive integer`);
  return parsed;
}

export const config = {
  databaseUrl: process.env.DATABASE_URL ?? "postgres://financial:financial@localhost:5432/financial",
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),
  port: integer("PORT", 3000),
  dbPoolMax: integer("DB_POOL_MAX", 20),
  transactionBatchSize: integer("TRANSACTION_BATCH_SIZE", 32),
  transactionBatchWindowMs: integer("TRANSACTION_BATCH_WINDOW_MS", 2),
  transactionBatchConcurrency: integer("TRANSACTION_BATCH_CONCURRENCY", 4),
  transactionBatchQueueMax: integer("TRANSACTION_BATCH_QUEUE_MAX", 5_000),
  logLevel: process.env.LOG_LEVEL ?? "info",
  relayBatchSize: integer("RELAY_BATCH_SIZE", 500),
  relayPollMs: integer("RELAY_POLL_MS", 250),
  relayLockTimeoutMs: integer("RELAY_LOCK_TIMEOUT_MS", 30_000),
} as const;

import { hostname } from "node:os";
import { Kafka, logLevel } from "kafkajs";
import { config } from "../../../packages/config/src/index.js";
import { pool } from "../../../packages/database/src/pool.js";
import { relayOnce } from "./relay.js";
import { startMetricsServer } from "../../../packages/observability/src/metrics.js";

const kafka = new Kafka({ clientId: "financial-outbox-relay", brokers: config.kafkaBrokers, logLevel: logLevel.WARN,
  retry: { retries: 8, initialRetryTime: 300 } });
const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 5, allowAutoTopicCreation: true });
await producer.connect();
const relayId = `${hostname()}-${process.pid}`;
const metricsServer = startMetricsServer(pool, Number.parseInt(process.env.RELAY_METRICS_PORT ?? "9091", 10));
let running = true;
const shutdown = () => { running = false; };
process.on("SIGINT", shutdown); process.on("SIGTERM", shutdown);
while (running) {
  try {
    const result = await relayOnce(pool, producer, { relayId, batchSize: config.relayBatchSize, staleLockTimeoutMs: config.relayLockTimeoutMs });
    if (result.claimed === 0) await new Promise((resolve) => setTimeout(resolve, config.relayPollMs));
  } catch (error) {
    console.error({ error }, "outbox relay batch failed");
    await new Promise((resolve) => setTimeout(resolve, config.relayPollMs));
  }
}
await producer.disconnect();
metricsServer.close();
await pool.end();

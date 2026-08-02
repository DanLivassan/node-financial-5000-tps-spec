import { Kafka, logLevel } from "kafkajs";
import { config } from "../../../packages/config/src/index.js";
import { pool } from "../../../packages/database/src/pool.js";
import type { FinancialEventEnvelope } from "../../../packages/contracts/src/events.js";
import { processEvent } from "./processor.js";
import { startMetricsServer } from "../../../packages/observability/src/metrics.js";

const topics = ["financial.transaction.accepted.v1", "financial.ledger.posted.v1", "financial.transaction.reversed.v1",
  "financial.reconciliation.completed.v1", "financial.reconciliation.divergence-detected.v1"];
const kafka = new Kafka({ clientId: "financial-event-consumer", brokers: config.kafkaBrokers, logLevel: logLevel.WARN });
const consumer = kafka.consumer({ groupId: process.env.KAFKA_CONSUMER_GROUP ?? "financial-audit-v1" });
const metricsServer = startMetricsServer(pool, Number.parseInt(process.env.CONSUMER_METRICS_PORT ?? "9093", 10));
await consumer.connect();
await consumer.subscribe({ topics, fromBeginning: false });
await consumer.run({ eachMessage: async ({ message }) => {
  if (!message.value) throw new Error("Kafka event has no value");
  const event = JSON.parse(message.value.toString()) as FinancialEventEnvelope;
  await processEvent(pool, process.env.KAFKA_CONSUMER_NAME ?? "financial-audit", event);
} });
const shutdown = async () => { metricsServer.close(); await consumer.disconnect(); await pool.end(); };
process.on("SIGINT", shutdown); process.on("SIGTERM", shutdown);

import { randomUUID } from "node:crypto";
import { Kafka, logLevel } from "kafkajs";
import { afterAll, describe, expect, it } from "vitest";
import { relayOnce } from "../apps/outbox-relay/src/relay.js";
import { processEvent } from "../apps/event-consumer/src/processor.js";
import { config } from "../packages/config/src/index.js";
import type { FinancialEventEnvelope } from "../packages/contracts/src/events.js";
import { pool } from "../packages/database/src/pool.js";

afterAll(() => pool.end());

describe("real Kafka delivery", () => {
  it("publishes a committed outbox row and consumes it idempotently", async () => {
    const suffix = randomUUID();
    const topic = `financial.integration.${suffix}`;
    const eventId = randomUUID();
    const aggregateId = randomUUID();
    await pool.query(
      `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload,created_at)
       VALUES ($1,'integration',$2::uuid,$3,1,$2::text,$4,now()-interval '1 day')`, [eventId, aggregateId, topic, { eventId, integration: true }],
    );
    const kafka = new Kafka({ clientId: `integration-${suffix}`, brokers: config.kafkaBrokers, logLevel: logLevel.NOTHING });
    const admin = kafka.admin();
    const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 5 });
    const consumer = kafka.consumer({ groupId: `integration-${suffix}` });
    await admin.connect();
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions: 12, replicationFactor: 1 }] });
    await admin.disconnect();
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    let resolveReceived!: () => void;
    const received = new Promise<void>((resolve) => { resolveReceived = resolve; });
    await consumer.run({ eachMessage: async ({ message }) => {
      const envelope = JSON.parse(message.value!.toString()) as FinancialEventEnvelope;
      await processEvent(pool, "kafka-integration-consumer", envelope);
      if (envelope.eventId === eventId) resolveReceived();
    } });
    await producer.connect();
    const relayed = await relayOnce(pool, producer, { relayId: `integration-${suffix}`, batchSize: 1_000, staleLockTimeoutMs: 30_000 });
    expect(relayed.published).toBeGreaterThan(0);
    await Promise.race([received, new Promise((_, reject) => setTimeout(() => reject(new Error("Kafka receive timeout")), 10_000))]);
    const proof = await pool.query<{ effects: bigint }>(
      "SELECT count(*) effects FROM processed_financial_events WHERE consumer_name=$1 AND event_id=$2",
      ["kafka-integration-consumer", eventId],
    );
    expect(proof.rows[0]!.effects).toBe(1n);
    await consumer.disconnect();
    await producer.disconnect();
  });
});

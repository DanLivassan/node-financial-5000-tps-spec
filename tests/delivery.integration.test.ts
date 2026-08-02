import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { processEvent } from "../apps/event-consumer/src/processor.js";
import { pool } from "../packages/database/src/pool.js";
import { claimOutboxBatch, markPublished } from "../packages/ledger/src/outbox.js";

afterAll(() => pool.end());

describe("at-least-once delivery foundations", () => {
  it("allows multiple relay replicas to claim disjoint bounded batches", async () => {
    for (let index = 0; index < 12; index += 1) {
      const id = randomUUID();
      await pool.query(
        `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload)
         VALUES ($1,'test',$2::uuid,'financial.transaction.accepted.v1',1,$2::text,$3)`,
        [id, randomUUID(), { testRun: id }],
      );
    }
    const [a, b] = await Promise.all([
      claimOutboxBatch(pool, "integration-relay-a", 6),
      claimOutboxBatch(pool, "integration-relay-b", 6),
    ]);
    expect(a).toHaveLength(6);
    expect(b).toHaveLength(6);
    expect(a.some((left) => b.some((right) => right.id === left.id))).toBe(false);
    await Promise.all([
      markPublished(pool, "integration-relay-a", a.map((event) => event.id)),
      markPublished(pool, "integration-relay-b", b.map((event) => event.id)),
    ]);
  });

  it("deduplicates a repeated Kafka event and applies its side effect once", async () => {
    const eventId = randomUUID();
    const event = { eventId, eventType: "financial.transaction.accepted.v1", eventVersion: 1,
      aggregateId: randomUUID(), partitionKey: randomUUID(), payload: { amountMinor: 100 }, headers: {} };
    expect(await processEvent(pool, "integration-consumer", event)).toEqual({ duplicate: false });
    expect(await processEvent(pool, "integration-consumer", event)).toEqual({ duplicate: true });
    const proof = await pool.query<{ consumed: bigint; effects: bigint }>(
      `SELECT (SELECT count(*) FROM consumed_events WHERE consumer_name=$1 AND event_id=$2) consumed,
       (SELECT count(*) FROM processed_financial_events WHERE consumer_name=$1 AND event_id=$2) effects`,
      ["integration-consumer", eventId],
    );
    expect(proof.rows[0]).toEqual({ consumed: 1n, effects: 1n });
  });
});

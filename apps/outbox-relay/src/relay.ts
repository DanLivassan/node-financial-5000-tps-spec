import { CompressionTypes, type Producer } from "kafkajs";
import type pg from "pg";
import { claimOutboxBatch, markFailed, markPublished, recoverStaleLocks } from "../../../packages/ledger/src/outbox.js";
import { metrics } from "../../../packages/observability/src/metrics.js";

export async function relayOnce(db: pg.Pool, producer: Pick<Producer, "sendBatch">, options: {
  relayId: string; batchSize: number; staleLockTimeoutMs: number;
}): Promise<{ claimed: number; published: number }> {
  await recoverStaleLocks(db, options.staleLockTimeoutMs);
  const events = await claimOutboxBatch(db, options.relayId, options.batchSize);
  if (events.length === 0) return { claimed: 0, published: 0 };
  const grouped = new Map<string, typeof events>();
  for (const event of events) grouped.set(event.event_type, [...(grouped.get(event.event_type) ?? []), event]);
  try {
    await producer.sendBatch({
      acks: -1,
      compression: CompressionTypes.GZIP,
      topicMessages: [...grouped.entries()].map(([topic, rows]) => ({
        topic,
        messages: rows.map((event) => ({
          key: event.partition_key,
          value: JSON.stringify({ eventId: event.id, eventType: event.event_type, eventVersion: event.event_version,
            aggregateId: event.aggregate_id, partitionKey: event.partition_key, payload: event.payload, headers: event.headers }),
          headers: { event_id: event.id, event_type: event.event_type, ...event.headers },
        })),
      })),
    });
    const published = await markPublished(db, options.relayId, events.map((event) => event.id));
    metrics.outboxPublished.inc(published);
    return { claimed: events.length, published };
  } catch (error) {
    metrics.outboxFailures.inc(events.length); metrics.kafkaProducerErrors.inc();
    await markFailed(db, options.relayId, events, error);
    throw error;
  }
}

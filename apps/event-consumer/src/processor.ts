import type pg from "pg";
import type { FinancialEventEnvelope } from "../../../packages/contracts/src/events.js";
import { metrics } from "../../../packages/observability/src/metrics.js";

export async function processEvent(db: pg.Pool, consumerName: string, event: FinancialEventEnvelope): Promise<{ duplicate: boolean }> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const claimed = await client.query(
      `INSERT INTO consumed_events(consumer_name,event_id) VALUES ($1,$2)
       ON CONFLICT DO NOTHING RETURNING event_id`, [consumerName, event.eventId],
    );
    if (claimed.rowCount === 0) { await client.query("COMMIT"); metrics.consumerDuplicates.inc({ consumer: consumerName }); return { duplicate: true }; }
    await client.query(
      `INSERT INTO processed_financial_events(consumer_name,event_id,event_type,aggregate_id,payload)
       VALUES ($1,$2,$3,$4,$5)`, [consumerName, event.eventId, event.eventType, event.aggregateId, event.payload],
    );
    await client.query("COMMIT");
    return { duplicate: false };
  } catch (error) { await client.query("ROLLBACK"); throw error; }
  finally { client.release(); }
}

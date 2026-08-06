import type pg from "pg";
import type { AcceptedCommand } from "./batch-simulator.js";

export async function persistOutbox(client: pg.PoolClient, accepted: AcceptedCommand[]): Promise<void> {
  if (accepted.length === 0) return;
  const rows = accepted.map((item) => ({
    id: item.eventId, aggregate_id: item.id, partition_key: item.command.request.sourceAccountId,
    payload: {
      eventId: item.eventId, transactionId: item.id, journalEntryId: item.journalEntryId,
      ...item.command.request, occurredAt: item.createdAt,
    },
    headers: {
      correlationId: item.id, schemaVersion: "1",
      ...(item.command.traceparent ? { traceparent: item.command.traceparent } : {}),
    },
  }));
  await client.query(
    `INSERT INTO outbox_events(id,aggregate_type,aggregate_id,event_type,event_version,partition_key,payload,headers)
     SELECT x.id,'financial_transaction',x.aggregate_id,'financial.transaction.accepted.v1',1,
       x.partition_key,x.payload,x.headers
     FROM jsonb_to_recordset($1::jsonb) AS x(
       id uuid,aggregate_id uuid,partition_key text,payload jsonb,headers jsonb)`,
    [JSON.stringify(rows)],
  );
}

import type pg from "pg";

export interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  event_version: number;
  partition_key: string;
  payload: Record<string, unknown>;
  headers: Record<string, string>;
  attempts: number;
  created_at: Date;
}

export function retryDelayMs(attempt: number, random = Math.random()): number {
  const capped = Math.min(Math.max(attempt, 1), 10);
  return Math.min(60_000, 100 * 2 ** (capped - 1)) + Math.floor(random * 250);
}

export async function recoverStaleLocks(db: pg.Pool, timeoutMs: number): Promise<number> {
  const result = await db.query(
    `UPDATE outbox_events SET status='failed',locked_at=NULL,locked_by=NULL,last_error='stale relay lock recovered',available_at=now()
     WHERE status='processing' AND locked_at < now() - ($1::int * interval '1 millisecond')`, [timeoutMs],
  );
  return result.rowCount ?? 0;
}

export async function claimOutboxBatch(db: pg.Pool, relayId: string, batchSize: number): Promise<OutboxRow[]> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query<OutboxRow>(
      `WITH selected AS (
         SELECT id FROM outbox_events
         WHERE status IN ('pending','failed') AND available_at <= now()
         ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT $1
       )
       UPDATE outbox_events o SET status='processing',locked_at=now(),locked_by=$2,attempts=attempts+1
       FROM selected WHERE o.id=selected.id
       RETURNING o.id,o.aggregate_id,o.event_type,o.event_version,o.partition_key,o.payload,o.headers,o.attempts,o.created_at`,
      [batchSize, relayId],
    );
    await client.query("COMMIT");
    return result.rows;
  } catch (error) { await client.query("ROLLBACK"); throw error; }
  finally { client.release(); }
}

export async function markPublished(db: pg.Pool, relayId: string, ids: readonly string[]): Promise<number> {
  if (ids.length === 0) return 0;
  const result = await db.query(
    `UPDATE outbox_events SET status='published',published_at=now(),locked_at=NULL,locked_by=NULL,last_error=NULL
     WHERE id=ANY($1::uuid[]) AND status='processing' AND locked_by=$2`, [ids, relayId],
  );
  return result.rowCount ?? 0;
}

export async function markFailed(db: pg.Pool, relayId: string, events: readonly OutboxRow[], error: unknown): Promise<number> {
  if (events.length === 0) return 0;
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    let updated = 0;
    for (const event of events) {
      const delay = retryDelayMs(event.attempts);
      const result = await client.query(
        `UPDATE outbox_events SET status='failed',available_at=now()+($1::int*interval '1 millisecond'),
         locked_at=NULL,locked_by=NULL,last_error=$2 WHERE id=$3 AND status='processing' AND locked_by=$4`,
        [delay, String(error).slice(0, 4000), event.id, relayId],
      );
      updated += result.rowCount ?? 0;
    }
    await client.query("COMMIT");
    return updated;
  } catch (failure) { await client.query("ROLLBACK"); throw failure; }
  finally { client.release(); }
}

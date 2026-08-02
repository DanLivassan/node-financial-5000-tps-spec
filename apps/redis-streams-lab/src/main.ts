import { createClient } from "redis";
import { pool } from "../../../packages/database/src/pool.js";

const redis = createClient({ url: process.env.REDIS_URL ?? "redis://localhost:6379" });
redis.on("error", (error) => console.error({ error }, "Redis Streams lab error"));
await redis.connect();
const stream = process.env.REDIS_STREAM ?? "financial-events-lab";
let running = true;
process.on("SIGINT", () => { running = false; }); process.on("SIGTERM", () => { running = false; });
while (running) {
  const rows = await pool.query<{ id: string; event_type: string; partition_key: string; payload: unknown }>(
    `SELECT o.id,o.event_type,o.partition_key,o.payload FROM outbox_events o
     LEFT JOIN redis_lab_deliveries d ON d.event_id=o.id WHERE d.event_id IS NULL AND o.status='published'
     ORDER BY o.created_at LIMIT 100`,
  );
  for (const event of rows.rows) {
    const streamId = await redis.xAdd(stream, "*", { event_id: event.id, event_type: event.event_type,
      partition_key: event.partition_key, payload: JSON.stringify(event.payload) });
    await pool.query(
      `INSERT INTO redis_lab_deliveries(event_id,stream_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, [event.id, streamId],
    );
  }
  if (rows.rowCount === 0) await new Promise((resolve) => setTimeout(resolve, 250));
}
await redis.quit(); await pool.end();

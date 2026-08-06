import type pg from "pg";
import { metrics } from "../../observability/src/metrics.js";

function isRetryable(error: unknown): boolean {
  const code = (error as { code?: string }).code;
  return code === "40001" || code === "40P01";
}

async function backoff(attempt: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 5 * 2 ** attempt + Math.random() * 10));
}

export async function withTransactionRetry<T>(
  db: pg.Pool, operation: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const client = await db.connect();
    try {
      await client.query("BEGIN");
      const result = await operation(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      if (!isRetryable(error) || attempt >= 2) throw error;
      const code = (error as { code: string }).code;
      metrics.balanceRetries.inc({ sqlstate: code });
      await backoff(attempt);
    } finally {
      client.release();
    }
  }
  throw new Error("batch transaction retry limit exhausted");
}

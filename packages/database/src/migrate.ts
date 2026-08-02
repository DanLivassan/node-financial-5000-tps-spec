import { readFile, readdir } from "node:fs/promises";
import { resolve } from "node:path";
import { pool } from "./pool.js";

const directory = resolve(process.cwd(), "migrations");
const files = (await readdir(directory)).filter((file) => file.endsWith(".sql")).sort();
const client = await pool.connect();
try {
  await client.query("BEGIN");
  await client.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
    name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now()
  )`);
  for (const file of files) {
    const claimed = await client.query(
      "INSERT INTO schema_migrations(name) VALUES ($1) ON CONFLICT DO NOTHING RETURNING name",
      [file],
    );
    if (claimed.rowCount === 1) await client.query(await readFile(resolve(directory, file), "utf8"));
  }
  await client.query("COMMIT");
  console.log(`Applied migrations: ${files.join(", ")}`);
} catch (error) {
  await client.query("ROLLBACK");
  throw error;
} finally {
  client.release();
  await pool.end();
}

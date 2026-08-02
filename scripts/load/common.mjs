import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import pg from "pg";
import autocannon from "autocannon";

export const databaseUrl = process.env.DATABASE_URL ?? "postgres://financial:financial@localhost:5432/financial";
export const targetUrl = process.env.LOAD_URL ?? "http://localhost:3000";

export async function accountPair() {
  if (process.env.SOURCE_ACCOUNT_ID && process.env.DESTINATION_ACCOUNT_ID) {
    return { sourceAccountId: process.env.SOURCE_ACCOUNT_ID, destinationAccountId: process.env.DESTINATION_ACCOUNT_ID };
  }
  const pool = new pg.Pool({ connectionString: databaseUrl });
  const result = await pool.query("SELECT id,code FROM ledger_accounts WHERE code IN ('CUSTOMER:A','CUSTOMER:B')");
  await pool.end();
  const ids = new Map(result.rows.map((row) => [row.code, row.id]));
  if (!ids.get("CUSTOMER:A") || !ids.get("CUSTOMER:B")) throw new Error("Run pnpm db:seed or set SOURCE_ACCOUNT_ID and DESTINATION_ACCOUNT_ID");
  return { sourceAccountId: ids.get("CUSTOMER:A"), destinationAccountId: ids.get("CUSTOMER:B") };
}

export function requestFor(pair, prefix = "load") {
  const id = randomUUID();
  return { key: `${prefix}-${id}`, body: JSON.stringify({ externalReference: `${prefix}-${id}`,
    sourceAccountId: pair.sourceAccountId, destinationAccountId: pair.destinationAccountId,
    amountMinor: Number.parseInt(process.env.LOAD_AMOUNT_MINOR ?? "1", 10), currency: "BRL" }) };
}

export function summary(result) {
  return { requestsPerSecond: result.requests.average, throughputBytesPerSecond: result.throughput.average,
    p50Ms: result.latency.p50, p95Ms: result.latency.p95, p99Ms: result.latency.p99,
    errors: result.errors, timeouts: result.timeouts, non2xx: result.non2xx, durationSeconds: result.duration };
}

export async function runAutocannon(options) {
  const latencies = [];
  const instance = autocannon(options);
  instance.on("response", (_client, _statusCode, _bytes, responseTime) => latencies.push(responseTime));
  const result = await instance;
  latencies.sort((a, b) => a - b);
  result.latency.p95 = latencies[Math.max(0, Math.ceil(latencies.length * 0.95) - 1)] ?? 0;
  return result;
}

export async function saveArtifact(kind, result, extra = {}) {
  const directory = new URL("../../artifacts/load-tests/", import.meta.url);
  await mkdir(directory, { recursive: true });
  const timestamp = new Date().toISOString().replaceAll(":", "-");
  const file = new URL(`${kind}-${timestamp}.json`, directory);
  const artifact = { kind, recordedAt: new Date().toISOString(), environment: {
    node: process.version, platform: `${os.platform()} ${os.release()} ${os.arch()}`,
    cpus: os.cpus().map((cpu) => cpu.model), cpuCount: os.availableParallelism(), memoryBytes: os.totalmem(),
    dbPoolMax: Number.parseInt(process.env.DB_POOL_MAX ?? "20", 10), kafkaPartitions: 12,
    payloadAmountMinor: Number.parseInt(process.env.LOAD_AMOUNT_MINOR ?? "1", 10),
    durability: { synchronousCommit: "on (not changed by benchmark)", fsync: "on (not changed by benchmark)", kafkaAcks: "all" },
  }, summary: summary(result), extra, raw: result };
  await writeFile(file, JSON.stringify(artifact, null, 2));
  console.log(JSON.stringify(artifact.summary, null, 2));
  console.log(`Raw artifact: ${file.pathname}`);
  return artifact;
}

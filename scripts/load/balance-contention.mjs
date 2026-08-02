import pg from "pg";
import { databaseUrl, requestFor, runAutocannon, saveArtifact, summary, targetUrl } from "./common.mjs";

const pool = new pg.Pool({ connectionString: databaseUrl });
const accountCount = Number.parseInt(process.env.CONTENTION_ACCOUNT_COUNT ?? "1000", 10);
if (!Number.isSafeInteger(accountCount) || accountCount < 2 || accountCount % 2 !== 0) {
  throw new Error("CONTENTION_ACCOUNT_COUNT must be an even integer >= 2");
}
const pairCount = accountCount / 2;
const duration = Number.parseInt(process.env.CONTENTION_DURATION ?? "10", 10);
const connections = Number.parseInt(process.env.CONTENTION_CONNECTIONS ?? "200", 10);
const runs = Number.parseInt(process.env.CONTENTION_RUNS ?? "3", 10);
const zipfAlpha = Number.parseFloat(process.env.CONTENTION_ZIPF_ALPHA ?? "1.1");
const seed = Number.parseInt(process.env.CONTENTION_SEED ?? "5000", 10) >>> 0;
const fundingMinor = Number.parseInt(process.env.CONTENTION_FUNDING_MINOR ?? "100000", 10);

await pool.query(
  `INSERT INTO ledger_accounts(code,account_type,currency)
   SELECT 'BENCH1000:SOURCE:' || value,'liability','BRL' FROM generate_series(0,$1::int-1) value
   UNION ALL
   SELECT 'BENCH1000:DEST:' || value,'liability','BRL' FROM generate_series(0,$1::int-1) value
   ON CONFLICT (code) DO NOTHING`, [pairCount],
);
const accountRows = await pool.query("SELECT id,code FROM ledger_accounts WHERE code LIKE 'BENCH1000:%'");
const ids = new Map(accountRows.rows.map((row) => [row.code, row.id]));
const pairs = Array.from({ length: pairCount }, (_, index) => ({
  sourceAccountId: ids.get(`BENCH1000:SOURCE:${index}`),
  destinationAccountId: ids.get(`BENCH1000:DEST:${index}`),
}));
if (pairs.some((pair) => !pair.sourceAccountId || !pair.destinationAccountId)) throw new Error("failed to create all benchmark accounts");

const fundingResult = await pool.query("SELECT id FROM ledger_accounts WHERE code='EQUITY:OPENING'");
const fundingAccountId = fundingResult.rows[0]?.id;
if (!fundingAccountId) throw new Error("Run pnpm db:seed before the contention benchmark");

for (let offset = 0; offset < pairs.length; offset += 20) {
  await Promise.all(pairs.slice(offset, offset + 20).map(async (pair) => {
    const generated = requestFor({ sourceAccountId: fundingAccountId, destinationAccountId: pair.sourceAccountId }, "bench1000-fund");
    const body = { ...JSON.parse(generated.body), amountMinor: fundingMinor };
    const response = await fetch(`${targetUrl}/v1/transactions`, { method: "POST",
      headers: { "content-type": "application/json", "idempotency-key": generated.key }, body: JSON.stringify(body) });
    if (!response.ok) throw new Error(`benchmark funding failed: ${response.status} ${await response.text()}`);
  }));
}

function mulberry32(initialSeed) {
  let state = initialSeed;
  return () => {
    state |= 0; state = state + 0x6D2B79F5 | 0;
    let value = Math.imul(state ^ state >>> 15, 1 | state);
    value = value + Math.imul(value ^ value >>> 7, 61 | value) ^ value;
    return ((value ^ value >>> 14) >>> 0) / 4294967296;
  };
}

const zipfCdf = [];
let zipfTotal = 0;
for (let rank = 1; rank <= pairCount; rank += 1) zipfTotal += 1 / rank ** zipfAlpha;
let cumulative = 0;
for (let rank = 1; rank <= pairCount; rank += 1) { cumulative += (1 / rank ** zipfAlpha) / zipfTotal; zipfCdf.push(cumulative); }
function zipfIndex(random) {
  let low = 0; let high = zipfCdf.length - 1;
  while (low < high) { const middle = Math.floor((low + high) / 2); if (random <= zipfCdf[middle]) high = middle; else low = middle + 1; }
  return low;
}

async function readOperationalCounters() {
  const database = await pool.query("SELECT deadlocks::bigint FROM pg_stat_database WHERE datname=current_database()");
  let retries = 0;
  try {
    const text = await (await fetch(`${targetUrl}/metrics`)).text();
    for (const match of text.matchAll(/^balance_update_retries_total(?:\{[^}]*\})?\s+([\d.e+-]+)$/gm)) retries += Number(match[1]);
  } catch { /* workload will expose API unavailability separately */ }
  return { deadlocks: BigInt(database.rows[0]?.deadlocks ?? 0), retries };
}

async function executeRun(scenario, runIndex, choosePair) {
  let sequence = 0;
  const result = await runAutocannon({ url: targetUrl, duration, connections, pipelining: 1,
    requests: [{ method: "POST", path: "/v1/transactions", body: "{}", headers: { "content-type": "application/json" },
      setupRequest: (request) => {
        const generated = requestFor(choosePair(sequence++), `bench1000-${scenario}-run${runIndex + 1}`);
        return { ...request, headers: { ...request.headers, "idempotency-key": generated.key }, body: generated.body };
      } }],
  });
  return result;
}

const countersBefore = await readOperationalCounters();
const rawRuns = { uniform: [], zipf: [], hot: [], mixed80_20: [] };
for (let runIndex = 0; runIndex < runs; runIndex += 1) {
  const zipfRandom = mulberry32(seed + runIndex);
  rawRuns.uniform.push(await executeRun("uniform", runIndex, (index) => pairs[index % pairCount]));
  rawRuns.zipf.push(await executeRun("zipf", runIndex, () => pairs[zipfIndex(zipfRandom())]));
  rawRuns.hot.push(await executeRun("hot", runIndex, () => pairs[0]));
  rawRuns.mixed80_20.push(await executeRun("mixed80_20", runIndex, (index) => index % 5 === 0 ? pairs[0] : pairs[index % pairCount]));
}
const countersAfter = await readOperationalCounters();

const discrepancy = await pool.query(`WITH derived AS (SELECT a.id,a.currency,COALESCE(sum(CASE
  WHEN a.account_type IN ('asset','expense') AND p.direction='debit' THEN p.amount_minor WHEN a.account_type IN ('asset','expense') THEN -p.amount_minor
  WHEN p.direction='credit' THEN p.amount_minor ELSE -p.amount_minor END),0)::bigint value FROM ledger_accounts a LEFT JOIN ledger_postings p ON p.account_id=a.id GROUP BY a.id,a.currency)
 SELECT count(*)::int count FROM derived d LEFT JOIN account_balances b ON b.account_id=d.id AND b.currency=d.currency WHERE COALESCE(b.available_minor,0)<>d.value`);

function aggregate(results) {
  const values = results.map(summary);
  const average = (field) => values.reduce((total, value) => total + value[field], 0) / values.length;
  const rpsMean = average("requestsPerSecond");
  const rpsVariance = values.reduce((total, value) => total + (value.requestsPerSecond - rpsMean) ** 2, 0) / values.length;
  return { runs: values.length, requestsPerSecondMean: rpsMean, requestsPerSecondStddev: Math.sqrt(rpsVariance),
    requestsPerSecondByRun: values.map((value) => value.requestsPerSecond), p50MsMean: average("p50Ms"),
    p95MsMean: average("p95Ms"), p99MsMean: average("p99Ms"),
    errors: values.reduce((total, value) => total + value.errors, 0),
    non2xx: values.reduce((total, value) => total + value.non2xx, 0) };
}
const scenarios = Object.fromEntries(Object.entries(rawRuns).map(([name, results]) => [name, aggregate(results)]));
const counterDeltas = { deadlocks: Number(countersAfter.deadlocks - countersBefore.deadlocks), retries: countersAfter.retries - countersBefore.retries };
const extra = { accountCount, pairCount, durationSecondsPerRun: duration, connections, seed, zipfAlpha, fundingMinor,
  distribution: { uniform: "round-robin over 500 independent pairs", zipf: `Zipf alpha=${zipfAlpha}`,
    hot: "100% on one pair", mixed80_20: "80% round-robin, 20% on one hot pair" },
  scenarios, counterDeltas, balanceDiscrepancies: discrepancy.rows[0].count, rawRuns };
await pool.end();

console.table(Object.fromEntries(Object.entries(scenarios).map(([name, result]) => [name, {
  rpsMean: result.requestsPerSecondMean.toFixed(2), rpsStddev: result.requestsPerSecondStddev.toFixed(2),
  p50Ms: result.p50MsMean.toFixed(2), p95Ms: result.p95MsMean.toFixed(2), p99Ms: result.p99MsMean.toFixed(2),
  errors: result.errors, non2xx: result.non2xx,
}])));
console.log({ counterDeltas, balanceDiscrepancies: discrepancy.rows[0].count });
await saveArtifact("account-distribution-1000", rawRuns.uniform[0], extra);
if (Object.values(scenarios).some((scenario) => scenario.errors || scenario.non2xx) ||
    counterDeltas.deadlocks > 0 || discrepancy.rows[0].count > 0) process.exitCode = 1;

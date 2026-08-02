import { accountPair, requestFor, runAutocannon, saveArtifact, targetUrl } from "./common.mjs";

const pair = await accountPair();
const duration = Number.parseInt(process.env.LOAD_DURATION ?? "10", 10);
const connections = Number.parseInt(process.env.LOAD_CONNECTIONS ?? "200", 10);
const pipelining = Number.parseInt(process.env.LOAD_PIPELINING ?? "1", 10);
const result = await runAutocannon({ url: targetUrl, duration, connections, pipelining,
  requests: [{ method: "POST", path: "/v1/transactions", body: "{}", headers: { "content-type": "application/json" },
    setupRequest: (request) => {
      const generated = requestFor(pair);
      return { ...request, headers: { ...request.headers, "idempotency-key": generated.key }, body: generated.body };
    } }],
});
const artifact = await saveArtifact("autocannon-10s", result, { targetUrl, connections, pipelining });
const minimumRps = Number.parseFloat(process.env.LOAD_MIN_RPS ?? "0");
const maximumP99 = Number.parseFloat(process.env.LOAD_MAX_P99_MS ?? "Infinity");
if (artifact.summary.requestsPerSecond < minimumRps || artifact.summary.p99Ms > maximumP99 || result.errors > 0 || result.non2xx > 0) process.exitCode = 1;

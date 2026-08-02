import Fastify, { LogController } from "fastify";
import type pg from "pg";
import { ZodError } from "zod";
import { transactionRequestSchema } from "../../../packages/contracts/src/transactions.js";
import { DomainError, createFinancialTransaction } from "../../../packages/ledger/src/transaction-service.js";
import { initializeDefaultMetrics, metrics, registry, updateDatabaseMetrics } from "../../../packages/observability/src/metrics.js";

export function buildServer(db: pg.Pool) {
  initializeDefaultMetrics();
  const app = Fastify({ logger: process.env.NODE_ENV === "test" ? false : {
    level: process.env.LOG_LEVEL ?? "info", redact: ["req.headers.authorization", "req.headers.idempotency-key"],
  }, logController: new LogController({ disableRequestLogging: true }), bodyLimit: 16 * 1024 });
  app.addHook("onRequest", async (request) => { (request as typeof request & { metricStartedAt: bigint }).metricStartedAt = process.hrtime.bigint(); });
  app.addHook("onResponse", async (request, reply) => {
    const route = request.routeOptions.url ?? "unknown";
    metrics.httpRequests.inc({ method: request.method, route, status_code: String(reply.statusCode) });
    const started = (request as typeof request & { metricStartedAt?: bigint }).metricStartedAt;
    if (started) metrics.httpDuration.observe({ method: request.method, route }, Number(process.hrtime.bigint() - started) / 1e9);
    const sampleRate = Number.parseFloat(process.env.LOG_SUCCESS_SAMPLE_RATE ?? "0.01");
    if (reply.statusCode >= 500 || Math.random() < sampleRate) request.log.info({ statusCode: reply.statusCode, route }, "request completed");
  });
  app.get("/health", async () => {
    await db.query("SELECT 1");
    return { status: "ok" };
  });
  app.get("/metrics", async (_request, reply) => {
    await updateDatabaseMetrics(db);
    return reply.header("content-type", registry.contentType).send(await registry.metrics());
  });
  app.post("/v1/transactions", async (request, reply) => {
    const keyHeader = request.headers["idempotency-key"];
    const key = Array.isArray(keyHeader) ? keyHeader[0] : keyHeader;
    const payload = transactionRequestSchema.parse(request.body);
    const traceHeader = request.headers.traceparent;
    const result = await createFinancialTransaction(db, key ?? "", payload,
      typeof traceHeader === "string" ? { traceparent: traceHeader } : {});
    reply.header("Idempotency-Replayed", String(result.replayed));
    return reply.code(result.statusCode).send(result.body);
  });
  app.setErrorHandler((error, _request, reply) => {
    if (error instanceof DomainError) return reply.code(error.httpStatus).send({ error: error.code, message: error.message });
    if (error instanceof ZodError) return reply.code(400).send({ error: "INVALID_REQUEST", details: error.issues });
    const httpError = error as { statusCode?: number; code?: string; message?: string };
    if (typeof httpError.statusCode === "number" && httpError.statusCode >= 400 && httpError.statusCode < 500) {
      return reply.code(httpError.statusCode).send({ error: httpError.code ?? "INVALID_REQUEST", message: httpError.message ?? "Invalid request" });
    }
    app.log.error(error);
    return reply.code(500).send({ error: "INTERNAL_ERROR" });
  });
  return app;
}

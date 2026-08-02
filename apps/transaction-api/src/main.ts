import { config } from "../../../packages/config/src/index.js";
import { pool } from "../../../packages/database/src/pool.js";
import { buildServer } from "./server.js";

const app = buildServer(pool);
const shutdown = async () => { await app.close(); await pool.end(); };
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
await app.listen({ host: "0.0.0.0", port: config.port });

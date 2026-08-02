import pg from "pg";
import { config } from "../../config/src/index.js";

pg.types.setTypeParser(20, (value) => BigInt(value));

export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: config.dbPoolMax,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
  application_name: "financial-platform",
});

export type DbClient = pg.PoolClient;

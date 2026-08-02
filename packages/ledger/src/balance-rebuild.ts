import type pg from "pg";

export interface BalanceDiscrepancy {
  account_id: string;
  currency: string;
  projected_minor: bigint;
  ledger_derived_minor: bigint;
}

export async function auditBalances(client: pg.Pool | pg.PoolClient, accountId?: string): Promise<BalanceDiscrepancy[]> {
  const result = await client.query<BalanceDiscrepancy>(
    `WITH derived AS (
       SELECT a.id account_id,a.currency,
         COALESCE(sum(CASE
           WHEN a.account_type IN ('asset','expense') AND p.direction='debit' THEN p.amount_minor
           WHEN a.account_type IN ('asset','expense') THEN -p.amount_minor
           WHEN p.direction='credit' THEN p.amount_minor ELSE -p.amount_minor END),0)::bigint ledger_derived_minor
       FROM ledger_accounts a LEFT JOIN ledger_postings p ON p.account_id=a.id AND p.currency=a.currency
       WHERE ($1::uuid IS NULL OR a.id=$1)
       GROUP BY a.id,a.currency
     )
     SELECT d.account_id,d.currency,COALESCE(b.available_minor,0)::bigint projected_minor,d.ledger_derived_minor
     FROM derived d LEFT JOIN account_balances b ON b.account_id=d.account_id AND b.currency=d.currency
     WHERE COALESCE(b.available_minor,0) <> d.ledger_derived_minor
     ORDER BY d.account_id`, [accountId ?? null],
  );
  return result.rows;
}

export async function recordAndRepairBalances(db: pg.Pool, accountId?: string): Promise<number> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const differences = await auditBalances(client, accountId);
    for (const row of differences) {
      await client.query(
        `INSERT INTO balance_audit_events(account_id,currency,projected_minor,ledger_derived_minor,repaired)
         VALUES ($1,$2,$3,$4,true)`,
        [row.account_id, row.currency, row.projected_minor, row.ledger_derived_minor],
      );
      await client.query(
        `INSERT INTO account_balances(account_id,currency,available_minor,version,updated_at)
         VALUES ($1,$2,$3,1,now()) ON CONFLICT (account_id,currency) DO UPDATE
         SET available_minor=EXCLUDED.available_minor,version=account_balances.version+1,updated_at=now()`,
        [row.account_id, row.currency, row.ledger_derived_minor],
      );
    }
    await client.query("COMMIT");
    return differences.length;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

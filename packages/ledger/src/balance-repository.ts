import type pg from "pg";

export interface BalanceDelta {
  accountId: string;
  currency: string;
  delta: bigint;
  versions: bigint;
}

function jsonRows(rows: unknown[]): string { return JSON.stringify(rows); }

export async function createAndLockBalances(
  client: pg.PoolClient, accountIds: string[],
): Promise<Map<string, bigint>> {
  if (accountIds.length === 0) return new Map();
  await client.query(
    `INSERT INTO account_balances(account_id,currency)
     SELECT id,currency FROM ledger_accounts WHERE id=ANY($1::uuid[])
     ON CONFLICT DO NOTHING`, [accountIds],
  );
  const locked = await client.query<{ account_id: string; available_minor: bigint }>(
    `SELECT account_id,available_minor FROM account_balances
     WHERE account_id=ANY($1::uuid[]) ORDER BY account_id FOR UPDATE`, [accountIds],
  );
  return new Map(locked.rows.map((row) => [row.account_id, row.available_minor]));
}

export async function applyBalanceDeltas(client: pg.PoolClient, deltas: BalanceDelta[]): Promise<void> {
  if (deltas.length === 0) return;
  await client.query(
    `UPDATE account_balances b SET
       available_minor=b.available_minor+x.delta_minor,
       version=b.version+x.version_increment,
       updated_at=now()
     FROM jsonb_to_recordset($1::jsonb) AS x(
       account_id uuid,currency text,delta_minor bigint,version_increment bigint)
     WHERE b.account_id=x.account_id AND b.currency=x.currency`,
    [jsonRows(deltas.map((item) => ({
      account_id: item.accountId, currency: item.currency,
      delta_minor: item.delta.toString(), version_increment: item.versions.toString(),
    })))],
  );
}

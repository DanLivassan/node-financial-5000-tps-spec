import type pg from "pg";
import type { AccountType } from "./domain.js";

export interface AccountRow {
  id: string;
  account_type: AccountType;
  status: "active" | "blocked";
  allow_negative: boolean;
  currency: string;
}

export async function loadAccounts(client: pg.PoolClient, accountIds: string[]): Promise<Map<string, AccountRow>> {
  if (accountIds.length === 0) return new Map();
  const result = await client.query<AccountRow>(
    `SELECT id,account_type,status,allow_negative,currency FROM ledger_accounts
     WHERE id=ANY($1::uuid[]) ORDER BY id FOR SHARE`,
    [accountIds],
  );
  return new Map(result.rows.map((row) => [row.id, row]));
}

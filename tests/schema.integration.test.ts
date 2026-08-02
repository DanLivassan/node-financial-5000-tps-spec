import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { pool } from "../packages/database/src/pool.js";

afterAll(() => pool.end());

describe("financial schema invariants", () => {
  it("accepts a balanced journal and rejects mutation", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const source = randomUUID();
      const destination = randomUUID();
      const transaction = randomUUID();
      const journal = randomUUID();
      await client.query(
        `INSERT INTO ledger_accounts(id, code, account_type, currency)
         VALUES ($1,$2,'liability','BRL'),($3,$4,'liability','BRL')`,
        [source, `test-${source}`, destination, `test-${destination}`],
      );
      await client.query(
        `INSERT INTO financial_transactions(
           id,idempotency_key,external_reference,source_account_id,destination_account_id,
           amount_minor,currency,status,request_hash,response_status,response_body)
         VALUES ($1,$2,$3,$4,$5,100,'BRL','accepted',$6,201,$7)`,
        [transaction, `test-${transaction}`, `ref-${transaction}`, source, destination, "a".repeat(64), { id: transaction }],
      );
      await client.query(
        `INSERT INTO journal_entries(id,transaction_id,external_reference,status,occurred_at)
         VALUES ($1,$2,$3,'posted',now())`,
        [journal, transaction, `ref-${transaction}`],
      );
      await client.query(
        `INSERT INTO ledger_postings(journal_entry_id,account_id,direction,amount_minor,currency,sequence)
         VALUES ($1,$2,'debit',100,'BRL',1),($1,$3,'credit',100,'BRL',2)`,
        [journal, source, destination],
      );
      await client.query("SET CONSTRAINTS ledger_postings_balanced IMMEDIATE");

      await client.query("SAVEPOINT mutation_check");
      await expect(client.query("UPDATE ledger_postings SET amount_minor=99 WHERE journal_entry_id=$1", [journal]))
        .rejects.toThrow(/immutable/);
      await client.query("ROLLBACK TO SAVEPOINT mutation_check");
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
  });

  it("rejects an unbalanced journal at constraint evaluation", async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      const source = randomUUID();
      const destination = randomUUID();
      const transaction = randomUUID();
      const journal = randomUUID();
      await client.query(
        `INSERT INTO ledger_accounts(id,code,account_type,currency) VALUES
         ($1,$2,'liability','BRL'),($3,$4,'liability','BRL')`,
        [source, `test-${source}`, destination, `test-${destination}`],
      );
      await client.query(
        `INSERT INTO financial_transactions(id,idempotency_key,external_reference,source_account_id,
          destination_account_id,amount_minor,currency,status,request_hash,response_status,response_body)
         VALUES ($1,$2,$3,$4,$5,100,'BRL','accepted',$6,201,$7)`,
        [transaction, `test-${transaction}`, `ref-${transaction}`, source, destination, "b".repeat(64), { id: transaction }],
      );
      await client.query(
        `INSERT INTO journal_entries(id,transaction_id,external_reference,status,occurred_at)
         VALUES ($1,$2,$3,'posted',now())`, [journal, transaction, `ref-${transaction}`],
      );
      await client.query(
        `INSERT INTO ledger_postings(journal_entry_id,account_id,direction,amount_minor,currency,sequence)
         VALUES ($1,$2,'debit',100,'BRL',1),($1,$3,'credit',99,'BRL',2)`,
        [journal, source, destination],
      );
      await expect(client.query("SET CONSTRAINTS ledger_postings_balanced IMMEDIATE"))
        .rejects.toThrow(/unbalanced/);
      await client.query("ROLLBACK");
    } finally {
      client.release();
    }
  });
});

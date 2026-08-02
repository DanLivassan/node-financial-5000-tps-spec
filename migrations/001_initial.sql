CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE ledger_accounts (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  code varchar(128) NOT NULL UNIQUE,
  account_type varchar(16) NOT NULL CHECK (account_type IN ('asset','liability','equity','revenue','expense')),
  currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  status varchar(16) NOT NULL DEFAULT 'active' CHECK (status IN ('active','blocked')),
  allow_negative boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE financial_transactions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key varchar(128) NOT NULL UNIQUE,
  external_reference varchar(128) NOT NULL,
  source_account_id uuid NOT NULL REFERENCES ledger_accounts(id),
  destination_account_id uuid NOT NULL REFERENCES ledger_accounts(id),
  amount_minor bigint NOT NULL CHECK (amount_minor > 0),
  currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  status varchar(32) NOT NULL CHECK (status IN ('accepted','rejected','reversed')),
  request_hash char(64) NOT NULL,
  response_status integer NOT NULL,
  response_body jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (source_account_id <> destination_account_id)
);
CREATE INDEX financial_transactions_external_reference_idx
  ON financial_transactions (external_reference, created_at);

CREATE TABLE journal_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id uuid REFERENCES financial_transactions(id),
  reversal_of_journal_id uuid REFERENCES journal_entries(id),
  external_reference varchar(128) NOT NULL,
  status varchar(16) NOT NULL CHECK (status IN ('posted','reversal')),
  occurred_at timestamptz NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK ((transaction_id IS NOT NULL) OR (reversal_of_journal_id IS NOT NULL)),
  UNIQUE NULLS NOT DISTINCT (transaction_id, reversal_of_journal_id)
);

CREATE TABLE ledger_postings (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  journal_entry_id uuid NOT NULL REFERENCES journal_entries(id),
  account_id uuid NOT NULL REFERENCES ledger_accounts(id),
  direction varchar(8) NOT NULL CHECK (direction IN ('debit','credit')),
  amount_minor bigint NOT NULL CHECK (amount_minor > 0),
  currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  sequence integer NOT NULL CHECK (sequence > 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (journal_entry_id, sequence)
);
CREATE INDEX ledger_postings_account_created_idx ON ledger_postings (account_id, created_at, id);

CREATE TABLE account_balances (
  account_id uuid NOT NULL REFERENCES ledger_accounts(id),
  currency char(3) NOT NULL,
  available_minor bigint NOT NULL DEFAULT 0,
  pending_minor bigint NOT NULL DEFAULT 0,
  version bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (account_id, currency)
);

CREATE TABLE outbox_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_type varchar(64) NOT NULL,
  aggregate_id uuid NOT NULL,
  event_type varchar(128) NOT NULL,
  event_version integer NOT NULL CHECK (event_version > 0),
  partition_key varchar(128) NOT NULL,
  payload jsonb NOT NULL,
  headers jsonb NOT NULL DEFAULT '{}'::jsonb,
  status varchar(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','failed','published')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  available_at timestamptz NOT NULL DEFAULT now(),
  locked_at timestamptz,
  locked_by varchar(128),
  published_at timestamptz,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_events_pending_idx ON outbox_events (available_at, created_at)
  WHERE status IN ('pending','failed');
CREATE INDEX outbox_events_processing_idx ON outbox_events (locked_at)
  WHERE status = 'processing';

CREATE TABLE consumed_events (
  consumer_name varchar(128) NOT NULL,
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE bank_statement_entries (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  provider varchar(64) NOT NULL,
  bank_account_id varchar(128) NOT NULL,
  provider_entry_id varchar(128) NOT NULL,
  provider_transaction_id varchar(128),
  end_to_end_id varchar(128),
  external_reference varchar(128),
  direction varchar(8) NOT NULL CHECK (direction IN ('debit','credit')),
  amount_minor bigint NOT NULL CHECK (amount_minor > 0),
  currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  occurred_at timestamptz NOT NULL,
  raw_payload jsonb NOT NULL,
  imported_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, bank_account_id, provider_entry_id)
);
CREATE INDEX bank_statement_matching_idx
  ON bank_statement_entries (provider, bank_account_id, occurred_at);

CREATE TABLE reconciliation_runs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  run_key char(64) NOT NULL UNIQUE,
  provider varchar(64) NOT NULL,
  bank_account_id varchar(128) NOT NULL,
  ledger_account_id uuid NOT NULL REFERENCES ledger_accounts(id),
  period_start timestamptz NOT NULL,
  period_end timestamptz NOT NULL,
  status varchar(16) NOT NULL CHECK (status IN ('running','completed','failed')),
  internal_opening_minor bigint,
  internal_closing_minor bigint,
  bank_opening_minor bigint,
  bank_closing_minor bigint,
  matched_count integer NOT NULL DEFAULT 0,
  unmatched_internal_count integer NOT NULL DEFAULT 0,
  unmatched_bank_count integer NOT NULL DEFAULT 0,
  mismatch_count integer NOT NULL DEFAULT 0,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  CHECK (period_end > period_start)
);

CREATE TABLE reconciliation_items (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  reconciliation_run_id uuid NOT NULL REFERENCES reconciliation_runs(id),
  transaction_id uuid REFERENCES financial_transactions(id),
  bank_statement_entry_id uuid REFERENCES bank_statement_entries(id),
  match_key varchar(256),
  status varchar(32) NOT NULL CHECK (status IN (
    'matched','missing_at_bank','missing_in_ledger','amount_mismatch','direction_mismatch',
    'duplicate_bank_entry','duplicate_internal_entry','late_settlement','manual_review','resolved'
  )),
  difference_minor bigint NOT NULL DEFAULT 0,
  reason varchar(128),
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  resolved_by varchar(128),
  resolved_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX reconciliation_items_identity_idx ON reconciliation_items (
  reconciliation_run_id,
  COALESCE(transaction_id, '00000000-0000-0000-0000-000000000000'::uuid),
  COALESCE(bank_statement_entry_id, '00000000-0000-0000-0000-000000000000'::uuid),
  status
);

CREATE TABLE balance_audit_events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  account_id uuid NOT NULL REFERENCES ledger_accounts(id),
  currency char(3) NOT NULL,
  projected_minor bigint NOT NULL,
  ledger_derived_minor bigint NOT NULL,
  repaired boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION reject_immutable_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is immutable; create a compensating journal entry', TG_TABLE_NAME
    USING ERRCODE = '55000';
END $$;

CREATE TRIGGER ledger_postings_immutable
  BEFORE UPDATE OR DELETE ON ledger_postings
  FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();
CREATE TRIGGER journal_entries_immutable
  BEFORE UPDATE OR DELETE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION reject_immutable_change();

CREATE OR REPLACE FUNCTION assert_journal_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE unbalanced_count integer;
BEGIN
  SELECT count(*) INTO unbalanced_count
  FROM (
    SELECT currency
    FROM ledger_postings
    WHERE journal_entry_id = NEW.journal_entry_id
    GROUP BY currency
    HAVING sum(CASE WHEN direction='debit' THEN amount_minor ELSE 0 END)
        <> sum(CASE WHEN direction='credit' THEN amount_minor ELSE 0 END)
  ) totals;
  IF unbalanced_count > 0 THEN
    RAISE EXCEPTION 'journal entry % is unbalanced', NEW.journal_entry_id USING ERRCODE = '23514';
  END IF;
  RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER ledger_postings_balanced
  AFTER INSERT ON ledger_postings
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION assert_journal_balanced();

CREATE OR REPLACE FUNCTION reject_posting_account_mismatch() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE account_currency char(3);
BEGIN
  SELECT currency INTO account_currency FROM ledger_accounts WHERE id = NEW.account_id;
  IF account_currency IS DISTINCT FROM NEW.currency THEN
    RAISE EXCEPTION 'posting currency does not match account currency' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER ledger_postings_currency
  BEFORE INSERT ON ledger_postings
  FOR EACH ROW EXECUTE FUNCTION reject_posting_account_mismatch();

ALTER TABLE financial_transactions ADD COLUMN end_to_end_id varchar(128);
ALTER TABLE financial_transactions ADD COLUMN provider_transaction_id varchar(128);
CREATE INDEX financial_transactions_end_to_end_idx ON financial_transactions (end_to_end_id) WHERE end_to_end_id IS NOT NULL;
CREATE INDEX financial_transactions_provider_tx_idx ON financial_transactions (provider_transaction_id) WHERE provider_transaction_id IS NOT NULL;

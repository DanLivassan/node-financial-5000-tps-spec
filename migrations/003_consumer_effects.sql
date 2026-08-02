CREATE TABLE processed_financial_events (
  consumer_name varchar(128) NOT NULL,
  event_id uuid NOT NULL,
  event_type varchar(128) NOT NULL,
  aggregate_id uuid NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id),
  FOREIGN KEY (consumer_name, event_id) REFERENCES consumed_events(consumer_name, event_id)
);

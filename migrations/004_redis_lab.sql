CREATE TABLE redis_lab_deliveries (
  event_id uuid PRIMARY KEY REFERENCES outbox_events(id),
  stream_id varchar(128) NOT NULL,
  delivered_at timestamptz NOT NULL DEFAULT now()
);

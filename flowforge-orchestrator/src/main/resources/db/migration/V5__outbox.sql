-- The transactional outbox. A message is written in the same transaction as the state change it
-- describes; OutboxRelay publishes it to Kafka afterwards and stamps published_at. parked_at
-- takes a row that can never be published out of the queue so it can't block every row behind it.
CREATE TABLE outbox (
  id               BIGSERIAL PRIMARY KEY,
  message_id       UUID NOT NULL UNIQUE,
  topic            VARCHAR(200) NOT NULL,
  message_key      VARCHAR(200) NOT NULL,
  message_type     VARCHAR(100) NOT NULL,
  payload          JSONB NOT NULL,
  headers          JSONB NOT NULL DEFAULT '{}',
  created_at       TIMESTAMPTZ NOT NULL,
  published_at     TIMESTAMPTZ,
  publish_attempts INT NOT NULL DEFAULT 0,
  last_error       VARCHAR(1000),
  parked_at        TIMESTAMPTZ   -- set when a send failed in a way retrying can't fix; the relay skips it
);
CREATE INDEX ix_outbox_unpublished ON outbox(id) WHERE published_at IS NULL AND parked_at IS NULL;
CREATE INDEX ix_outbox_published ON outbox(published_at) WHERE published_at IS NOT NULL;

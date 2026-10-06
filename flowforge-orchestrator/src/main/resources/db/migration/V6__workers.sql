-- The worker registry. One row per worker incarnation (<name>-<8 hex>, new on every start).
CREATE TABLE workers (
  id              VARCHAR(100) PRIMARY KEY,
  name            VARCHAR(64)  NOT NULL,
  hostname        VARCHAR(255) NOT NULL,
  status          VARCHAR(20)  NOT NULL CHECK (status IN ('REGISTERING','ACTIVE','DRAINING','OFFLINE')),
  capabilities    TEXT[] NOT NULL,
  max_concurrency INT NOT NULL,
  current_load    INT NOT NULL DEFAULT 0,
  registered_at   TIMESTAMPTZ NOT NULL,
  last_seen_at    TIMESTAMPTZ NOT NULL,
  offline_at      TIMESTAMPTZ
);
CREATE INDEX ix_workers_status ON workers(status);

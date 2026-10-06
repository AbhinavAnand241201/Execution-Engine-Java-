-- Dead-letter queue: one row per task that gave up (FAILED_PERMANENTLY), until an operator replays it.
CREATE TABLE dead_letter_entries (
  id                    UUID PRIMARY KEY,
  task_execution_id     UUID NOT NULL REFERENCES task_executions(id),
  workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
  attempt               INT NOT NULL,
  error_code            VARCHAR(50),
  error_message         VARCHAR(2000),
  created_at            TIMESTAMPTZ NOT NULL,
  replayed_at           TIMESTAMPTZ,
  replayed_by           UUID REFERENCES users(id)
);
CREATE INDEX ix_dlq_open ON dead_letter_entries(created_at DESC) WHERE replayed_at IS NULL;
CREATE INDEX ix_dlq_execution ON dead_letter_entries(workflow_execution_id);

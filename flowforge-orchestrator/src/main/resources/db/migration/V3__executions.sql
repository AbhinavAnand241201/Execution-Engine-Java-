-- Executions, their tasks, finished attempts and the event timeline.
CREATE TABLE workflow_executions (
  id                  UUID PRIMARY KEY,
  workflow_id         UUID NOT NULL REFERENCES workflows(id),
  workflow_version_id UUID NOT NULL REFERENCES workflow_versions(id),
  owner_id            UUID NOT NULL REFERENCES users(id),
  status              VARCHAR(20) NOT NULL,
  input               JSONB,
  idempotency_key     VARCHAR(100),
  request_hash        CHAR(64),                        -- SHA-256 of the canonical request body
  correlation_id      VARCHAR(64) NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL,
  started_at          TIMESTAMPTZ,
  completed_at        TIMESTAMPTZ,
  updated_at          TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX ux_exec_idempotency ON workflow_executions(owner_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX ix_exec_owner_created ON workflow_executions(owner_id, created_at DESC);
CREATE INDEX ix_exec_workflow_created ON workflow_executions(workflow_id, created_at DESC);
CREATE INDEX ix_exec_status ON workflow_executions(status);

CREATE TABLE task_executions (
  id                    UUID PRIMARY KEY,
  workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
  task_definition_id    UUID NOT NULL REFERENCES task_definitions(id),
  task_key              VARCHAR(64) NOT NULL,
  type                  VARCHAR(20) NOT NULL,
  priority              SMALLINT NOT NULL,             -- weight: CRITICAL 40, HIGH 30, NORMAL 20, LOW 10
  status                VARCHAR(24) NOT NULL,
  attempt               INT NOT NULL DEFAULT 0,        -- fencing token, +1 on every dispatch
  retry_count           INT NOT NULL DEFAULT 0,        -- failures in current cycle, reset on replay
  max_attempts          INT NOT NULL,
  retry_policy          JSONB NOT NULL,                -- snapshot at execution start
  timeout_ms            BIGINT NOT NULL,
  worker_id             VARCHAR(100),
  ready_at              TIMESTAMPTZ,
  queued_at             TIMESTAMPTZ,
  started_at            TIMESTAMPTZ,
  deadline_at           TIMESTAMPTZ,
  next_attempt_at       TIMESTAMPTZ,
  completed_at          TIMESTAMPTZ,
  output                JSONB,
  error_code            VARCHAR(50),
  error_message         VARCHAR(2000),
  updated_at            TIMESTAMPTZ NOT NULL,
  UNIQUE (workflow_execution_id, task_key)             -- the logical idempotency key of a task execution
);
CREATE INDEX ix_task_ready      ON task_executions(type, priority DESC, ready_at) WHERE status = 'READY';
CREATE INDEX ix_task_running_w  ON task_executions(worker_id)       WHERE status = 'RUNNING';
CREATE INDEX ix_task_deadline   ON task_executions(deadline_at)     WHERE status = 'RUNNING';
CREATE INDEX ix_task_retry_due  ON task_executions(next_attempt_at) WHERE status = 'RETRYING';
CREATE INDEX ix_task_queued     ON task_executions(queued_at)       WHERE status = 'QUEUED';

CREATE TABLE task_attempts (
  id                UUID PRIMARY KEY,
  task_execution_id UUID NOT NULL REFERENCES task_executions(id),
  attempt           INT NOT NULL,
  worker_id         VARCHAR(100),
  outcome           VARCHAR(20) NOT NULL,
  started_at        TIMESTAMPTZ,
  finished_at       TIMESTAMPTZ NOT NULL,
  error_code        VARCHAR(50),
  error_message     VARCHAR(2000),
  UNIQUE (task_execution_id, attempt)                  -- one outcome per attempt, enforced by the DB
);

CREATE TABLE workflow_events (
  id                    BIGSERIAL PRIMARY KEY,
  event_id              UUID NOT NULL UNIQUE,
  workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
  task_execution_id     UUID,
  type                  VARCHAR(50) NOT NULL,
  data                  JSONB,
  occurred_at           TIMESTAMPTZ NOT NULL
);
CREATE INDEX ix_events_exec ON workflow_events(workflow_execution_id, id);

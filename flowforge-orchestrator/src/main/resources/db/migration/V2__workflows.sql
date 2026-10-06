-- Workflows and their immutable versions.
CREATE TABLE workflows (
  id                       UUID PRIMARY KEY,
  owner_id                 UUID NOT NULL REFERENCES users(id),
  name                     VARCHAR(100) NOT NULL,
  description              VARCHAR(1000),
  latest_version           INT NOT NULL,
  latest_published_version INT,
  created_at               TIMESTAMPTZ NOT NULL,
  updated_at               TIMESTAMPTZ NOT NULL,
  UNIQUE (owner_id, name)
);

CREATE TABLE workflow_versions (
  id           UUID PRIMARY KEY,
  workflow_id  UUID NOT NULL REFERENCES workflows(id),
  version      INT  NOT NULL,
  status       VARCHAR(20) NOT NULL CHECK (status IN ('DRAFT','PUBLISHED')),
  definition   JSONB NOT NULL,
  lock_version BIGINT NOT NULL DEFAULT 0,             -- JPA @Version (optimistic locking of draft edits)
  created_at   TIMESTAMPTZ NOT NULL,
  published_at TIMESTAMPTZ,
  UNIQUE (workflow_id, version)
);
-- At most one editable version per workflow.
CREATE UNIQUE INDEX ux_one_draft_per_workflow ON workflow_versions(workflow_id) WHERE status = 'DRAFT';

-- Defense in depth: even a bug or a manual SQL session can't change or delete a published version, because running
-- executions point at it.
CREATE FUNCTION reject_published_version_change() RETURNS trigger AS $$
BEGIN
  IF OLD.status = 'PUBLISHED' THEN
    RAISE EXCEPTION 'workflow version % is published and immutable', OLD.id USING ERRCODE = 'restrict_violation';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_versions_immutable
  BEFORE UPDATE OR DELETE ON workflow_versions
  FOR EACH ROW EXECUTE FUNCTION reject_published_version_change();

-- Normalised copies of a published definition, written once at publish time; executions are created from these.
CREATE TABLE task_definitions (
  id                  UUID PRIMARY KEY,
  workflow_version_id UUID NOT NULL REFERENCES workflow_versions(id),
  task_key            VARCHAR(64) NOT NULL,
  type                VARCHAR(20) NOT NULL,
  config              JSONB NOT NULL,
  timeout_ms          BIGINT NOT NULL,
  priority            VARCHAR(10) NOT NULL,
  retry_policy        JSONB NOT NULL,
  UNIQUE (workflow_version_id, task_key)
);

CREATE TABLE task_dependencies (
  workflow_version_id UUID NOT NULL REFERENCES workflow_versions(id),
  task_key            VARCHAR(64) NOT NULL,
  depends_on_task_key VARCHAR(64) NOT NULL,
  PRIMARY KEY (workflow_version_id, task_key, depends_on_task_key)
);

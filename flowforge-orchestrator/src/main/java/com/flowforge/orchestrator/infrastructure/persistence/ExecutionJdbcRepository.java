package com.flowforge.orchestrator.infrastructure.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Explicit SQL for executions and their tasks (engine state uses JdbcTemplate, not JPA). Rows are
 * returned as plain records so callers can't accidentally dirty-check and write back engine state.
 */
@Repository
public class ExecutionJdbcRepository {

    private final JdbcTemplate jdbc;

    public ExecutionJdbcRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record ExecutionRow(UUID id, UUID workflowId, UUID workflowVersionId, int version, UUID ownerId, String status,
                               String input, String idempotencyKey, String requestHash, String correlationId,
                               Instant createdAt, Instant startedAt, Instant completedAt) {
    }

    public record TaskRow(UUID id, UUID workflowExecutionId, String taskKey, String type, int priority, String status,
                          int attempt, int retryCount, int maxAttempts, String retryPolicy, long timeoutMs, String workerId,
                          Instant readyAt, Instant queuedAt, Instant startedAt, Instant deadlineAt, Instant nextAttemptAt,
                          Instant completedAt, String output, String errorCode, String errorMessage) {
    }

    /** One row of {@code task_definitions} of a published version. */
    public record TaskDefinitionRow(UUID id, String taskKey, String type, String priority, String config, long timeoutMs,
                                    String retryPolicy) {
    }

    public record EventRow(long id, UUID eventId, UUID taskExecutionId, String type, String data, Instant occurredAt) {
    }

    public record AttemptRow(int attempt, String workerId, String outcome, Instant startedAt, Instant finishedAt,
                             String errorCode, String errorMessage) {
    }

    public record NewTask(UUID id, UUID taskDefinitionId, String taskKey, String type, int priority, String status,
                          int maxAttempts, String retryPolicy, long timeoutMs, Instant readyAt) {
    }

    public record NewEvent(UUID taskExecutionId, String type, String data) {
    }

    // ---- writes ------------------------------------------------------------------------------------------------

    public void insertExecution(UUID id, UUID workflowId, UUID versionId, UUID ownerId, String status, String input,
                                String idempotencyKey, String requestHash, String correlationId, Instant now) {
        jdbc.update("INSERT INTO workflow_executions (id, workflow_id, workflow_version_id, owner_id, status, input, "
                + "idempotency_key, request_hash, correlation_id, created_at, started_at, updated_at) "
                + "VALUES (?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?)",
            id, workflowId, versionId, ownerId, status, input, idempotencyKey, requestHash, correlationId, ts(now), ts(now),
            ts(now));
    }

    public void insertTasks(UUID executionId, List<NewTask> tasks, Instant now) {
        jdbc.batchUpdate("INSERT INTO task_executions (id, workflow_execution_id, task_definition_id, task_key, type, "
                + "priority, status, max_attempts, retry_policy, timeout_ms, ready_at, updated_at) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?)",
            tasks.stream().map(t -> new Object[] {t.id(), executionId, t.taskDefinitionId(), t.taskKey(), t.type(),
                t.priority(), t.status(), t.maxAttempts(), t.retryPolicy(), t.timeoutMs(), ts(t.readyAt()), ts(now)})
                .toList());
    }

    /** Timeline rows; {@code eventIds.get(i)} is the id of {@code events.get(i)} (also its outbox message id). */
    public void insertEvents(UUID executionId, List<UUID> eventIds, List<NewEvent> events, Instant now) {
        List<Object[]> rows = new ArrayList<>();
        for (int i = 0; i < events.size(); i++) {
            NewEvent e = events.get(i);
            rows.add(new Object[] {eventIds.get(i), executionId, e.taskExecutionId(), e.type(), e.data(), ts(now)});
        }
        jdbc.batchUpdate("INSERT INTO workflow_events (event_id, workflow_execution_id, task_execution_id, type, data, "
            + "occurred_at) VALUES (?, ?, ?, ?, ?::jsonb, ?)", rows);
    }

    // ---- reads -------------------------------------------------------------------------------------------------

    private static final String EXECUTION_COLUMNS = "e.id, e.workflow_id, e.workflow_version_id, v.version, e.owner_id, "
        + "e.status, e.input::text AS input, e.idempotency_key, e.request_hash, e.correlation_id, e.created_at, "
        + "e.started_at, e.completed_at FROM workflow_executions e JOIN workflow_versions v ON v.id = e.workflow_version_id";

    public Optional<ExecutionRow> findExecution(UUID id) {
        return jdbc.query("SELECT " + EXECUTION_COLUMNS + " WHERE e.id = ?", ExecutionJdbcRepository::execution, id)
            .stream().findFirst();
    }

    public Optional<ExecutionRow> findByIdempotencyKey(UUID ownerId, String key) {
        return jdbc.query("SELECT " + EXECUTION_COLUMNS + " WHERE e.owner_id = ? AND e.idempotency_key = ?",
            ExecutionJdbcRepository::execution, ownerId, key).stream().findFirst();
    }

    /**
     * @param ownerId   null = all owners (OPERATOR/ADMIN)
     * @param orderBy   a whitelisted column expression, never user text
     */
    public List<ExecutionRow> listExecutions(UUID ownerId, UUID workflowId, String status, Instant createdFrom,
                                             Instant createdTo, String orderBy, int limit, long offset) {
        List<Object> args = new ArrayList<>();
        String where = filters(ownerId, workflowId, status, createdFrom, createdTo, args);
        args.add(limit);
        args.add(offset);
        return jdbc.query("SELECT " + EXECUTION_COLUMNS + where + " ORDER BY " + orderBy + " LIMIT ? OFFSET ?",
            ExecutionJdbcRepository::execution, args.toArray());
    }

    public long countExecutions(UUID ownerId, UUID workflowId, String status, Instant createdFrom, Instant createdTo) {
        List<Object> args = new ArrayList<>();
        String where = filters(ownerId, workflowId, status, createdFrom, createdTo, args);
        Long count = jdbc.queryForObject("SELECT count(*) FROM workflow_executions e" + where, Long.class, args.toArray());
        return count == null ? 0 : count;
    }

    private static String filters(UUID ownerId, UUID workflowId, String status, Instant createdFrom, Instant createdTo,
                                  List<Object> args) {
        StringBuilder where = new StringBuilder(" WHERE 1 = 1");
        if (ownerId != null) {
            where.append(" AND e.owner_id = ?");
            args.add(ownerId);
        }
        if (workflowId != null) {
            where.append(" AND e.workflow_id = ?");
            args.add(workflowId);
        }
        if (status != null) {
            where.append(" AND e.status = ?");
            args.add(status);
        }
        if (createdFrom != null) {
            where.append(" AND e.created_at >= ?");
            args.add(ts(createdFrom));
        }
        if (createdTo != null) {
            where.append(" AND e.created_at < ?");
            args.add(ts(createdTo));
        }
        return where.toString();
    }

    public List<TaskRow> findTasks(UUID executionId) {
        return jdbc.query("SELECT id, workflow_execution_id, task_key, type, priority, status, attempt, retry_count, "
            + "max_attempts, retry_policy::text AS retry_policy, timeout_ms, worker_id, ready_at, queued_at, started_at, "
            + "deadline_at, next_attempt_at, completed_at, output::text AS output, error_code, error_message "
            + "FROM task_executions WHERE workflow_execution_id = ? ORDER BY task_key", ExecutionJdbcRepository::task,
            executionId);
    }

    public List<TaskDefinitionRow> findTaskDefinitions(UUID workflowVersionId) {
        return jdbc.query("SELECT id, task_key, type, priority, config::text AS config, timeout_ms, "
            + "retry_policy::text AS retry_policy FROM task_definitions WHERE workflow_version_id = ? ORDER BY task_key",
            (rs, i) -> new TaskDefinitionRow(rs.getObject("id", UUID.class), rs.getString("task_key"), rs.getString("type"),
                rs.getString("priority"), rs.getString("config"), rs.getLong("timeout_ms"), rs.getString("retry_policy")),
            workflowVersionId);
    }

    public List<EventRow> findEvents(UUID executionId, int limit, long offset) {
        return jdbc.query("SELECT id, event_id, task_execution_id, type, data::text AS data, occurred_at FROM workflow_events "
                + "WHERE workflow_execution_id = ? ORDER BY id LIMIT ? OFFSET ?",
            (rs, i) -> new EventRow(rs.getLong("id"), rs.getObject("event_id", UUID.class),
                rs.getObject("task_execution_id", UUID.class), rs.getString("type"), rs.getString("data"),
                instant(rs, "occurred_at")), executionId, limit, offset);
    }

    public long countEvents(UUID executionId) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM workflow_events WHERE workflow_execution_id = ?", Long.class,
            executionId);
        return count == null ? 0 : count;
    }

    public List<AttemptRow> findAttempts(UUID executionId, String taskKey) {
        return jdbc.query("SELECT a.attempt, a.worker_id, a.outcome, a.started_at, a.finished_at, a.error_code, "
                + "a.error_message FROM task_attempts a JOIN task_executions t ON t.id = a.task_execution_id "
                + "WHERE t.workflow_execution_id = ? AND t.task_key = ? ORDER BY a.attempt",
            (rs, i) -> new AttemptRow(rs.getInt("attempt"), rs.getString("worker_id"), rs.getString("outcome"),
                instant(rs, "started_at"), instant(rs, "finished_at"), rs.getString("error_code"),
                rs.getString("error_message")), executionId, taskKey);
    }

    public boolean taskExists(UUID executionId, String taskKey) {
        Integer count = jdbc.queryForObject("SELECT count(*) FROM task_executions WHERE workflow_execution_id = ? "
            + "AND task_key = ?", Integer.class, executionId, taskKey);
        return count != null && count > 0;
    }

    // ---- mapping -----------------------------------------------------------------------------------------------

    static ExecutionRow execution(ResultSet rs, int row) throws SQLException {
        return new ExecutionRow(rs.getObject("id", UUID.class), rs.getObject("workflow_id", UUID.class),
            rs.getObject("workflow_version_id", UUID.class), rs.getInt("version"), rs.getObject("owner_id", UUID.class),
            rs.getString("status"), rs.getString("input"), rs.getString("idempotency_key"), rs.getString("request_hash"),
            rs.getString("correlation_id"), instant(rs, "created_at"), instant(rs, "started_at"),
            instant(rs, "completed_at"));
    }

    static TaskRow task(ResultSet rs, int row) throws SQLException {
        return new TaskRow(rs.getObject("id", UUID.class), rs.getObject("workflow_execution_id", UUID.class),
            rs.getString("task_key"), rs.getString("type"), rs.getInt("priority"), rs.getString("status"),
            rs.getInt("attempt"), rs.getInt("retry_count"), rs.getInt("max_attempts"), rs.getString("retry_policy"),
            rs.getLong("timeout_ms"), rs.getString("worker_id"), instant(rs, "ready_at"), instant(rs, "queued_at"),
            instant(rs, "started_at"), instant(rs, "deadline_at"), instant(rs, "next_attempt_at"),
            instant(rs, "completed_at"), rs.getString("output"), rs.getString("error_code"), rs.getString("error_message"));
    }

    static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}

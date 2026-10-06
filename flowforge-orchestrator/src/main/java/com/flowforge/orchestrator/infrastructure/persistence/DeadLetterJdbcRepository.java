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
 * The dead-letter queue: one row per task that gave up. It is a table, not a Kafka topic, so operators can
 * page, filter and replay entries, and a replay is an ordinary locked state change of the execution.
 */
@Repository
public class DeadLetterJdbcRepository {

    private static final int MAX_ERROR_MESSAGE = 2_000;
    private static final int MAX_ERROR_CODE = 50;

    private static final String SELECT = """
        SELECT d.id, d.task_execution_id, d.workflow_execution_id, w.workflow_id, t.task_key, d.attempt, d.error_code,
               d.error_message, d.created_at, d.replayed_at, d.replayed_by
        FROM dead_letter_entries d
        JOIN task_executions t ON t.id = d.task_execution_id
        JOIN workflow_executions w ON w.id = d.workflow_execution_id
        """;

    private final JdbcTemplate jdbc;

    public DeadLetterJdbcRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record DeadLetterRow(UUID id, UUID taskExecutionId, UUID workflowExecutionId, UUID workflowId, String taskKey,
                                int attempt, String errorCode, String errorMessage, Instant createdAt,
                                Instant replayedAt, UUID replayedBy) {
    }

    public void insert(UUID taskId, UUID executionId, int attempt, String errorCode, String errorMessage, Instant now) {
        jdbc.update("INSERT INTO dead_letter_entries (id, task_execution_id, workflow_execution_id, attempt, error_code, "
                + "error_message, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)", UUID.randomUUID(), taskId, executionId,
            attempt, EngineJdbcRepository.truncate(errorCode, MAX_ERROR_CODE),
            EngineJdbcRepository.truncate(errorMessage, MAX_ERROR_MESSAGE), Timestamp.from(now));
    }

    public Optional<DeadLetterRow> find(UUID entryId) {
        return jdbc.query(SELECT + " WHERE d.id = ?", DeadLetterJdbcRepository::row, entryId).stream().findFirst();
    }

    /**
     * Marks the execution's open entries replayed ({@code taskId} null = all of them). Returns how many were open, so
     * two concurrent replays of one entry can't both succeed (the second sees 0).
     */
    public int markReplayed(UUID executionId, UUID taskId, UUID operator, Instant now) {
        return jdbc.update("UPDATE dead_letter_entries SET replayed_at = ?, replayed_by = ? "
                + "WHERE workflow_execution_id = ? AND replayed_at IS NULL AND (?::uuid IS NULL OR task_execution_id = ?)",
            Timestamp.from(now), operator, executionId, taskId, taskId);
    }

    /** By creation time ({@code newestFirst} or oldest first). {@code workflowId} / {@code errorCode} null = no filter. */
    public List<DeadLetterRow> list(UUID workflowId, String errorCode, boolean includeReplayed, boolean newestFirst,
                                    int limit, long offset) {
        List<Object> args = new ArrayList<>();
        String where = where(workflowId, errorCode, includeReplayed, args);
        args.add(limit);
        args.add(offset);
        // Only a constant reaches the SQL: the direction is a boolean, not client text.
        String order = newestFirst ? " ORDER BY d.created_at DESC, d.id" : " ORDER BY d.created_at ASC, d.id";
        return jdbc.query(SELECT + where + order + " LIMIT ? OFFSET ?",
            DeadLetterJdbcRepository::row, args.toArray());
    }

    public long count(UUID workflowId, String errorCode, boolean includeReplayed) {
        List<Object> args = new ArrayList<>();
        String where = where(workflowId, errorCode, includeReplayed, args);
        Long count = jdbc.queryForObject("SELECT count(*) FROM dead_letter_entries d "
            + "JOIN workflow_executions w ON w.id = d.workflow_execution_id" + where, Long.class, args.toArray());
        return count == null ? 0 : count;
    }

    private static String where(UUID workflowId, String errorCode, boolean includeReplayed, List<Object> args) {
        StringBuilder where = new StringBuilder(" WHERE TRUE");
        if (!includeReplayed) {
            where.append(" AND d.replayed_at IS NULL");
        }
        if (workflowId != null) {
            where.append(" AND w.workflow_id = ?");
            args.add(workflowId);
        }
        if (errorCode != null) {
            where.append(" AND d.error_code = ?");
            args.add(errorCode);
        }
        return where.toString();
    }

    private static DeadLetterRow row(ResultSet rs, int n) throws SQLException {
        return new DeadLetterRow(rs.getObject("id", UUID.class), rs.getObject("task_execution_id", UUID.class),
            rs.getObject("workflow_execution_id", UUID.class), rs.getObject("workflow_id", UUID.class),
            rs.getString("task_key"), rs.getInt("attempt"), rs.getString("error_code"), rs.getString("error_message"),
            instant(rs, "created_at"), instant(rs, "replayed_at"), rs.getObject("replayed_by", UUID.class));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }
}

package com.flowforge.runtime.claim;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;
import javax.sql.DataSource;

/**
 * Claims one attempt of a task: QUEUED → RUNNING in a single conditional UPDATE. If two workers race,
 * PostgreSQL row-locks the task row; the second waits, re-checks the WHERE clause against the committed row (now
 * RUNNING) and updates nothing. So exactly one claim wins, with no read-then-write gap and no workflow lock (it can't
 * deadlock with the workflow-row-then-task-rows lock order). The attempt in the WHERE clause fences out stale and duplicate commands; the
 * join on the execution makes claims fail while it is PAUSED or CANCELLING.
 */
public class TaskClaimRepository {

    static final String CLAIM_SQL = """
        UPDATE task_executions t
        SET status = 'RUNNING', worker_id = ?, started_at = ?,
            deadline_at = ?::timestamptz + (t.timeout_ms * INTERVAL '1 millisecond'), updated_at = ?
        FROM workflow_executions w
        WHERE t.id = ?
          AND t.attempt = ?
          AND t.status = 'QUEUED'
          AND w.id = t.workflow_execution_id
          AND w.status = 'RUNNING'
        """;

    private final DataSource dataSource;

    public TaskClaimRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /** True iff this call moved the attempt to RUNNING for {@code workerId}. Runs in its own (auto-commit) transaction. */
    public boolean claim(UUID taskExecutionId, int attempt, String workerId, Instant now) {
        // An explicit UTC offset, so the JVM's default time zone can't shift the timestamps.
        OffsetDateTime at = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement update = connection.prepareStatement(CLAIM_SQL)) {
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            update.setString(1, workerId);
            update.setObject(2, at);
            update.setObject(3, at);
            update.setObject(4, at);
            update.setObject(5, taskExecutionId);
            update.setInt(6, attempt);
            return update.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new IllegalStateException("claim failed for task " + taskExecutionId, e);
        }
    }
}

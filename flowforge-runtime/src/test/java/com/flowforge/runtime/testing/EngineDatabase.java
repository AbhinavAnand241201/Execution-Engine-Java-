package com.flowforge.runtime.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * The real engine schema on the shared PostgreSQL container: the orchestrator's Flyway migrations, applied once per
 * JVM, plus helpers that insert an execution with tasks in a chosen state. Runtime code (claims, inputs) is tested
 * against exactly the tables production uses.
 */
public final class EngineDatabase {

    private static DataSource dataSource;

    private EngineDatabase() {
    }

    public static synchronized DataSource dataSource() {
        if (dataSource == null) {
            PGSimpleDataSource pg = new PGSimpleDataSource();
            pg.setUrl(IntegrationTestBase.POSTGRES.getJdbcUrl());
            pg.setUser(IntegrationTestBase.POSTGRES.getUsername());
            pg.setPassword(IntegrationTestBase.POSTGRES.getPassword());
            Flyway.configure().dataSource(pg)
                .locations("filesystem:../flowforge-orchestrator/src/main/resources/db/migration")
                .load().migrate();
            dataSource = pg;
        }
        return dataSource;
    }

    /** An execution with the given status and one task per key (all with the given status, attempt and timeout). */
    public static Execution insertExecution(String executionStatus, String inputJson, String taskStatus, int attempt,
                                            long timeoutMs, String... taskKeys) {
        UUID user = UUID.randomUUID();
        UUID workflow = UUID.randomUUID();
        UUID version = UUID.randomUUID();
        UUID execution = UUID.randomUUID();
        Timestamp now = Timestamp.from(Instant.now());
        Jdbc jdbc = new Jdbc(dataSource());
        jdbc.update("INSERT INTO users VALUES (?, ?, 'x', 'USER', ?, ?)", user, "u-" + user + "@t.local", now, now);
        jdbc.update("INSERT INTO workflows VALUES (?, ?, ?, NULL, 1, 1, ?, ?)", workflow, user, "wf-" + workflow, now, now);
        jdbc.update("INSERT INTO workflow_versions (id, workflow_id, version, status, definition, created_at) "
            + "VALUES (?, ?, 1, 'DRAFT', '{}'::jsonb, ?)", version, workflow, now);
        jdbc.update("INSERT INTO workflow_executions (id, workflow_id, workflow_version_id, owner_id, status, input, "
            + "correlation_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?::jsonb, 'corr', ?, ?)",
            execution, workflow, version, user, executionStatus, inputJson, now, now);
        UUID[] tasks = new UUID[taskKeys.length];
        for (int i = 0; i < taskKeys.length; i++) {
            UUID definition = UUID.randomUUID();
            tasks[i] = UUID.randomUUID();
            jdbc.update("INSERT INTO task_definitions VALUES (?, ?, ?, 'DELAY', '{}'::jsonb, ?, 'NORMAL', '{}'::jsonb)",
                definition, version, taskKeys[i], timeoutMs);
            jdbc.update("INSERT INTO task_executions (id, workflow_execution_id, task_definition_id, task_key, type, "
                + "priority, status, attempt, max_attempts, retry_policy, timeout_ms, updated_at) "
                + "VALUES (?, ?, ?, ?, 'DELAY', 20, ?, ?, 3, '{}'::jsonb, ?, ?)",
                tasks[i], execution, definition, taskKeys[i], taskStatus, attempt, timeoutMs, now);
        }
        return new Execution(execution, tasks);
    }

    public static void setTask(UUID task, String status, String outputJson) {
        new Jdbc(dataSource()).update("UPDATE task_executions SET status = ?, output = ?::jsonb WHERE id = ?", status,
            outputJson, task);
    }

    public static void setExecutionStatus(UUID execution, String status) {
        new Jdbc(dataSource()).update("UPDATE workflow_executions SET status = ? WHERE id = ?", status, execution);
    }

    public static TaskRow task(UUID task) {
        return new Jdbc(dataSource()).one("SELECT status, attempt, worker_id, started_at, deadline_at FROM task_executions "
            + "WHERE id = ?", rs -> new TaskRow(rs.getString(1), rs.getInt(2), rs.getString(3),
            rs.getTimestamp(4) == null ? null : rs.getTimestamp(4).toInstant(),
            rs.getTimestamp(5) == null ? null : rs.getTimestamp(5).toInstant()), task);
    }

    public record Execution(UUID id, UUID[] tasks) {
    }

    public record TaskRow(String status, int attempt, String workerId, Instant startedAt, Instant deadlineAt) {
    }

    static final ObjectMapper JSON = new ObjectMapper();
}

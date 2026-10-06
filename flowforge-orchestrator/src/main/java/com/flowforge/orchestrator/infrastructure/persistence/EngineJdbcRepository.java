package com.flowforge.orchestrator.infrastructure.persistence;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import com.flowforge.core.workflow.TaskType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * The engine's state transitions as explicit SQL. Every write is conditional on the state the caller
 * expects and returns the row count, so "someone else already moved it" is visible and treated as a no-op. Callers
 * that change an execution's state must first hold its row lock ({@link #lockExecution}), always before task rows.
 */
@Repository
public class EngineJdbcRepository {

    private static final int MAX_ERROR_MESSAGE = 2_000;
    private static final int MAX_ERROR_CODE = 50;

    private final JdbcTemplate jdbc;

    public EngineJdbcRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record LockedExecution(UUID id, String status, UUID workflowVersionId, String correlationId) {
    }

    public record TaskState(UUID id, String taskKey, String status) {
    }

    /** A READY task of a RUNNING execution, in dispatch order. */
    public record Candidate(UUID id, UUID workflowExecutionId, TaskType type) {
    }

    /** What a {@code TaskCommand} needs, read in the dispatch transaction. */
    public record CommandData(UUID taskExecutionId, UUID workflowExecutionId, String taskKey, TaskType type, String config,
                              List<String> dependsOn, long timeoutMs, String correlationId) {
    }

    /** What the failure path needs to know about one task, read after taking the execution lock. */
    public record TaskSnapshot(UUID id, UUID workflowExecutionId, String taskKey, String status, int attempt,
                               String workerId, int retryCount, String retryPolicy, Instant startedAt,
                               Instant deadlineAt) {
    }

    /** A task picked by a background job (plain SELECT); the job locks its execution, then re-reads it. */
    public record JobCandidate(UUID taskId, UUID workflowExecutionId) {
    }

    /** The execution row lock: serializes every state change of one execution. */
    public Optional<LockedExecution> lockExecution(UUID executionId) {
        return jdbc.query("SELECT id, status, workflow_version_id, correlation_id FROM workflow_executions WHERE id = ? "
                + "FOR UPDATE",
            (rs, n) -> new LockedExecution(rs.getObject(1, UUID.class), rs.getString(2), rs.getObject(3, UUID.class),
                rs.getString(4)), executionId).stream().findFirst();
    }

    /**
     * RUNNING → {@code to} for exactly this attempt on this worker. Empty if the row isn't in that state any more
     * (duplicate result, zombie attempt, or already timed out), otherwise the task key.
     */
    public Optional<String> finishTask(UUID taskId, int attempt, String workerId, String to, String output,
                                       String errorCode, String errorMessage, Instant now) {
        return jdbc.query("""
                UPDATE task_executions
                SET status = ?, completed_at = ?, output = ?::jsonb, error_code = ?, error_message = ?, updated_at = ?
                WHERE id = ? AND status = 'RUNNING' AND attempt = ? AND worker_id = ?
                RETURNING task_key
                """, (rs, n) -> rs.getString(1), to, ts(now), output, truncate(errorCode, MAX_ERROR_CODE),
            truncate(errorMessage, MAX_ERROR_MESSAGE), ts(now), taskId, attempt, workerId).stream().findFirst();
    }

    /** Plain read of one task. Stable while the caller holds the execution lock: only lock holders change it then. */
    public Optional<TaskSnapshot> findTask(UUID taskId) {
        return jdbc.query("SELECT id, workflow_execution_id, task_key, status, attempt, worker_id, retry_count, "
                + "retry_policy::text, started_at, deadline_at FROM task_executions WHERE id = ?",
            (rs, n) -> new TaskSnapshot(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getString(3),
                rs.getString(4), rs.getInt(5), rs.getString(6), rs.getInt(7), rs.getString(8), instant(rs, 9),
                instant(rs, 10)), taskId)
            .stream().findFirst();
    }

    /** RUNNING → RETRYING for exactly this attempt: one more failure counted, due again at {@code nextAttemptAt}. */
    public int scheduleRetry(UUID taskId, int attempt, String workerId, Instant nextAttemptAt, String errorCode,
                             String errorMessage, Instant now) {
        return jdbc.update("""
                UPDATE task_executions
                SET status = 'RETRYING', retry_count = retry_count + 1, next_attempt_at = ?, error_code = ?,
                    error_message = ?, updated_at = ?
                WHERE id = ? AND status = 'RUNNING' AND attempt = ? AND worker_id = ?
                """, ts(nextAttemptAt), truncate(errorCode, MAX_ERROR_CODE), truncate(errorMessage, MAX_ERROR_MESSAGE),
            ts(now), taskId, attempt, workerId);
    }

    /**
     * RETRYING tasks that are due, oldest first: plain SELECT. Executions being cancelled are left out, or their
     * tasks (which the promoter skips) could fill every batch and starve the rest.
     */
    public List<JobCandidate> dueRetries(Instant now, int limit) {
        return jdbc.query("SELECT t.id, t.workflow_execution_id FROM task_executions t "
                + "JOIN workflow_executions w ON w.id = t.workflow_execution_id "
                + "WHERE t.status = 'RETRYING' AND t.next_attempt_at <= ? "
                + "AND w.status NOT IN ('CANCELLING', 'CANCELLED', 'COMPLETED') "
                + "ORDER BY t.next_attempt_at, t.id LIMIT ?",
            (rs, n) -> new JobCandidate(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class)), ts(now), limit);
    }

    /** RETRYING → READY if it is (still) due. Empty if another promoter or a cancel got there first. */
    public Optional<String> promoteRetry(UUID taskId, Instant now) {
        return jdbc.query("UPDATE task_executions SET status = 'READY', ready_at = ?, next_attempt_at = NULL, "
                + "updated_at = ? WHERE id = ? AND status = 'RETRYING' AND next_attempt_at <= ? RETURNING task_key",
            (rs, n) -> rs.getString(1), ts(now), ts(now), taskId, ts(now)).stream().findFirst();
    }

    /** A RUNNING attempt to recover: which task, of which execution, which attempt. */
    public record RunningAttempt(UUID taskId, UUID workflowExecutionId, int attempt) {
    }

    /** RUNNING attempts claimed by this worker: plain SELECT; recovery then locks and re-checks each one. */
    public List<RunningAttempt> runningOn(String workerId) {
        return jdbc.query("SELECT id, workflow_execution_id, attempt FROM task_executions WHERE status = 'RUNNING' "
                + "AND worker_id = ? ORDER BY id",
            (rs, n) -> new RunningAttempt(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getInt(3)),
            workerId);
    }

    /**
     * Workers OFFLINE since before {@code cutoff} that still have RUNNING attempts: recovery that was interrupted
     * (orchestrator crash between marking OFFLINE and recovering) or never triggered (a graceful exit whose attempts
     * couldn't report). Plain SELECT.
     */
    public List<String> offlineWorkersWithRunningTasks(Instant cutoff) {
        return jdbc.queryForList("SELECT DISTINCT t.worker_id FROM task_executions t JOIN workers w ON w.id = t.worker_id "
            + "WHERE t.status = 'RUNNING' AND w.status = 'OFFLINE' AND w.offline_at < ?", String.class, ts(cutoff));
    }

    /**
     * QUEUED tasks of RUNNING executions dispatched before {@code cutoff} and still not claimed: plain SELECT.
     * Paused / cancelling executions are left out (their claims would fail anyway) so they can't fill the batch.
     */
    public List<JobCandidate> staleQueued(Instant cutoff, int limit) {
        return jdbc.query("SELECT t.id, t.workflow_execution_id FROM task_executions t "
                + "JOIN workflow_executions w ON w.id = t.workflow_execution_id "
                + "WHERE t.status = 'QUEUED' AND t.queued_at < ? AND w.status = 'RUNNING' "
                + "ORDER BY t.queued_at, t.id LIMIT ?",
            (rs, n) -> new JobCandidate(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class)), ts(cutoff), limit);
    }

    /** Restarts the "queued for too long" clock of this attempt before its command is sent again. */
    public int touchQueued(UUID taskId, int attempt, Instant now) {
        return jdbc.update("UPDATE task_executions SET queued_at = ?, updated_at = ? "
            + "WHERE id = ? AND status = 'QUEUED' AND attempt = ?", ts(now), ts(now), taskId, attempt);
    }

    /** RUNNING tasks whose deadline passed before {@code cutoff} (deadline + grace): plain SELECT. */
    public List<JobCandidate> overdueRunning(Instant cutoff, int limit) {
        return jdbc.query("SELECT id, workflow_execution_id FROM task_executions WHERE status = 'RUNNING' "
                + "AND deadline_at < ? ORDER BY deadline_at, id LIMIT ?",
            (rs, n) -> new JobCandidate(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class)), ts(cutoff), limit);
    }

    /** FAILED_PERMANENTLY → READY for a replay: a fresh retry cycle ({@code retry_count}, reset on replay). */
    public Optional<String> replayTask(UUID taskId, Instant now) {
        return jdbc.query("""
                UPDATE task_executions
                SET status = 'READY', retry_count = 0, ready_at = ?, next_attempt_at = NULL, completed_at = NULL,
                    error_code = NULL, error_message = NULL, updated_at = ?
                WHERE id = ? AND status = 'FAILED_PERMANENTLY'
                RETURNING task_key
                """, (rs, n) -> rs.getString(1), ts(now), ts(now), taskId).stream().findFirst();
    }

    /** One row per finished attempt; UNIQUE (task, attempt) is the backstop against applying a result twice. */
    public void insertAttempt(UUID taskId, int attempt, String workerId, String outcome, Instant startedAt,
                              Instant finishedAt, String errorCode, String errorMessage) {
        jdbc.update("INSERT INTO task_attempts (id, task_execution_id, attempt, worker_id, outcome, started_at, finished_at, "
                + "error_code, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", UUID.randomUUID(), taskId, attempt,
            workerId, outcome, ts(startedAt), ts(finishedAt), truncate(errorCode, MAX_ERROR_CODE),
            truncate(errorMessage, MAX_ERROR_MESSAGE));
    }

    /** All tasks of an execution by key; read after taking the execution lock, so it sees every committed result. */
    public Map<String, TaskState> taskStates(UUID executionId) {
        Map<String, TaskState> states = new LinkedHashMap<>();
        jdbc.query("SELECT id, task_key, status FROM task_executions WHERE workflow_execution_id = ? ORDER BY task_key",
            (ResultSet rs) -> {
                TaskState state = new TaskState(rs.getObject(1, UUID.class), rs.getString(2), rs.getString(3));
                states.put(state.taskKey(), state);
            }, executionId);
        return states;
    }

    /** PENDING → READY (dependencies satisfied). */
    public int markReady(UUID taskId, Instant now) {
        return jdbc.update("UPDATE task_executions SET status = 'READY', ready_at = ?, updated_at = ? "
            + "WHERE id = ? AND status = 'PENDING'", ts(now), ts(now), taskId);
    }

    public int setExecutionStatus(UUID executionId, String from, String to, Instant now, boolean finished) {
        return jdbc.update("UPDATE workflow_executions SET status = ?, updated_at = ?, "
                + "completed_at = CASE WHEN ? THEN ? ELSE completed_at END WHERE id = ? AND status = ?",
            to, ts(now), finished, ts(now), executionId, from);
    }

    /** FAILED → RUNNING for a replay; the execution is no longer finished. */
    public int reopenFailedExecution(UUID executionId, Instant now) {
        return jdbc.update("UPDATE workflow_executions SET status = 'RUNNING', completed_at = NULL, updated_at = ? "
            + "WHERE id = ? AND status = 'FAILED'", ts(now), executionId);
    }

    /** Plain SELECT, no locks: each candidate is then locked and re-checked in its own transaction. */
    public List<Candidate> readyCandidates(int limit) {
        return jdbc.query("""
                SELECT t.id, t.workflow_execution_id, t.type
                FROM task_executions t JOIN workflow_executions w ON w.id = t.workflow_execution_id
                WHERE t.status = 'READY' AND w.status = 'RUNNING'
                ORDER BY t.priority DESC, t.ready_at ASC, t.id
                LIMIT ?
                """, (rs, n) -> new Candidate(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class),
            TaskType.valueOf(rs.getString(3))), limit);
    }

    /** READY → QUEUED with a new attempt number (the fencing token of the command about to be sent). */
    public Optional<Integer> queueTask(UUID taskId, Instant now) {
        return jdbc.query("UPDATE task_executions SET status = 'QUEUED', attempt = attempt + 1, queued_at = ?, "
                + "worker_id = NULL, started_at = NULL, deadline_at = NULL, updated_at = ? "
                + "WHERE id = ? AND status = 'READY' RETURNING attempt",
            (rs, n) -> rs.getInt(1), ts(now), ts(now), taskId).stream().findFirst();
    }

    public CommandData commandData(UUID taskId) {
        return jdbc.queryForObject("""
                SELECT t.id, t.workflow_execution_id, t.task_key, t.type, d.config::text, t.timeout_ms, w.correlation_id,
                       ARRAY(SELECT dep.depends_on_task_key FROM task_dependencies dep
                             WHERE dep.workflow_version_id = d.workflow_version_id AND dep.task_key = t.task_key
                             ORDER BY dep.depends_on_task_key) AS depends_on
                FROM task_executions t
                JOIN task_definitions d ON d.id = t.task_definition_id
                JOIN workflow_executions w ON w.id = t.workflow_execution_id
                WHERE t.id = ?
                """, (rs, n) -> new CommandData(rs.getObject(1, UUID.class), rs.getObject(2, UUID.class), rs.getString(3),
            TaskType.valueOf(rs.getString(4)), rs.getString(5), strings(rs, "depends_on"), rs.getLong(6),
            rs.getString(7)), taskId);
    }

    /** Tasks dispatched but not finished (QUEUED + RUNNING) per type: what capacity is measured against. */
    public Map<TaskType, Integer> inFlightByType() {
        Map<TaskType, Integer> counts = new EnumMap<>(TaskType.class);
        jdbc.query("SELECT type, count(*) FROM task_executions WHERE status IN ('QUEUED', 'RUNNING') GROUP BY type",
            (ResultSet rs) -> {
                counts.put(TaskType.valueOf(rs.getString(1)), rs.getInt(2));
            });
        return counts;
    }

    private static List<String> strings(ResultSet rs, String column) throws SQLException {
        java.sql.Array array = rs.getArray(column);
        return array == null ? List.of() : List.of((String[]) array.getArray());
    }

    private static Instant instant(ResultSet rs, int column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    static String truncate(String text, int max) {
        return text == null || text.length() <= max ? text : text.substring(0, max);
    }

    private static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }
}

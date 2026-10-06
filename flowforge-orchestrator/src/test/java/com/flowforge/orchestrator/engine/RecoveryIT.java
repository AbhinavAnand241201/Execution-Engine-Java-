package com.flowforge.orchestrator.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.RedisKeys;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.application.PublishedDagCache;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.orchestrator.testing.Runs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Worker failure handling driven by hand (jobs off): recovery of a dead worker's attempts, the monitor's behaviour
 * when Redis is down, capacity from the registry, and a crash inside result processing.
 */
class RecoveryIT extends OrchestratorIntegrationTestBase {

    private static String token;

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private RecoveryService recovery;
    @Autowired
    private WorkerMonitor monitor;
    @Autowired
    private WorkerJdbcRepository workers;
    @Autowired
    private EngineJdbcRepository engine;
    @Autowired
    private EventPublisher publisher;
    @Autowired
    private PublishedDagCache dags;
    @Autowired
    private FailureHandler failures;
    @Autowired
    private TransactionTemplate transaction;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private ReadyTaskSignal readyTasks;
    @Autowired
    private ResultProcessor results;
    @Autowired
    private MeterRegistry meters;
    @Autowired
    private StringRedisTemplate redisTemplate;

    private Runs runs;
    private final List<String> created = new ArrayList<>();
    private final List<String> workerIds = new ArrayList<>();

    @BeforeEach
    void setUp() {
        token = token == null ? api().newUserToken() : token;
        runs = new Runs(api(), token);
    }

    @AfterEach
    void cleanUp() {
        for (String id : created) {
            jdbc.update("UPDATE task_executions SET status = 'CANCELLED' WHERE workflow_execution_id = ?::uuid "
                + "AND status IN ('PENDING', 'READY', 'QUEUED', 'RUNNING', 'RETRYING')", id);
            jdbc.update("UPDATE workflow_executions SET status = 'CANCELLED' WHERE id = ?::uuid "
                + "AND status NOT IN ('COMPLETED', 'FAILED')", id);
        }
        workerIds.forEach(id -> jdbc.update("DELETE FROM workers WHERE id = ?", id));
    }

    /** A single-task workflow with 3 attempts and no backoff. */
    private String workflow() {
        Map<String, Object> task = new HashMap<>(Runs.task("only", "DELAY", Map.of("durationMs", 0), null));
        task.put("retry", Map.of("maxAttempts", 3, "backoff", "FIXED", "initialDelayMs", 0, "maxDelayMs", 0,
            "jitter", false));
        return runs.publish(Map.of("tasks", List.of(task)));
    }

    private UUID runningOn(String workflowId, String workerId, int attempt) {
        String id = runs.execute(workflowId, null);
        created.add(id);
        jdbc.update("UPDATE task_executions SET status = 'RUNNING', attempt = ?, worker_id = ?, started_at = now() "
            + "WHERE workflow_execution_id = ?::uuid", attempt, workerId, id);
        return UUID.fromString(id);
    }

    private Map<String, Object> task(UUID execution) {
        return jdbc.queryForMap("SELECT id, status, attempt, retry_count FROM task_executions "
            + "WHERE workflow_execution_id = ?", execution);
    }

    private String worker(String status, Instant registeredAt, String... capabilities) {
        String id = "it-" + UUID.randomUUID().toString().substring(0, 8);
        workerIds.add(id);
        jdbc.update("INSERT INTO workers (id, name, hostname, status, capabilities, max_concurrency, registered_at, "
                + "last_seen_at) VALUES (?, 'it', 'localhost', ?, ?, 4, ?, ?)", id, status, capabilities,
            Timestamp.from(registeredAt), Timestamp.from(registeredAt));
        return id;
    }

    @Test
    @DisplayName("T17.3 a task that kills every worker it runs on → WORKER_LOST counts toward maxAttempts → "
        + "FAILED_PERMANENTLY after 3 attempts (no endless loop)")
    void poisonTaskStops() {
        String workflowId = workflow();
        UUID execution = runningOn(workflowId, "doomed-1", 1);

        for (int attempt = 1; attempt <= 3; attempt++) {
            String doomed = "doomed-" + attempt;
            jdbc.update("UPDATE task_executions SET status = 'RUNNING', attempt = ?, worker_id = ?, started_at = now() "
                + "WHERE workflow_execution_id = ?", attempt, doomed, execution);
            assertThat(recovery.recoverTasksOf(doomed)).isEqualTo(1);
        }

        assertThat(task(execution)).containsEntry("status", "FAILED_PERMANENTLY");
        assertThat(jdbc.queryForList("SELECT outcome FROM task_attempts WHERE task_execution_id = ? ORDER BY attempt",
            String.class, task(execution).get("id"))).containsExactly("WORKER_LOST", "WORKER_LOST", "WORKER_LOST");
        assertThat(jdbc.queryForObject("SELECT status FROM workflow_executions WHERE id = ?", String.class, execution))
            .isEqualTo("FAILED");
    }

    @Test
    @DisplayName("T17.5 two monitors recover the same offline worker at once (start gate, 50 iterations) → exactly "
        + "one WORKER_LOST attempt per task")
    void concurrentRecovery() throws Exception {
        String workflowId = workflow();
        List<UUID> executions = new ArrayList<>();
        List<String> deadWorkers = new ArrayList<>();

        List<List<Object>> outcomes = ConcurrencyHarness.race(2, 50, thread -> recovery.recoverTasksOf(
            deadWorkers.getLast()), () -> {
                String dead = "dead-" + UUID.randomUUID().toString().substring(0, 8);
                deadWorkers.add(dead);
                executions.add(runningOn(workflowId, dead, 1));
            });

        for (int i = 0; i < 50; i++) {
            assertThat(outcomes.get(i).stream().mapToInt(o -> (Integer) o).sum()).as("iteration %d", i).isEqualTo(1);
            Map<String, Object> task = task(executions.get(i));
            assertThat(task).containsEntry("status", "RETRYING").containsEntry("retry_count", 1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM task_attempts WHERE task_execution_id = ? "
                + "AND outcome = 'WORKER_LOST'", Integer.class, task.get("id"))).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("T17.6 a crash inside result processing after the task update, before commit → rolled back; the "
        + "redelivered result is applied once")
    void crashBeforeCommit() {
        UUID execution = runningOn(workflow(), "w-crash", 1);
        UUID taskId = (UUID) task(execution).get("id");
        AtomicBoolean crashed = new AtomicBoolean();
        ResultProcessor crashing = new ResultProcessor(engine, publisher, dags, failures, transaction,
            Clock.systemUTC(), mapper, readyTasks, new SimpleMeterRegistry(), () -> {
                if (crashed.compareAndSet(false, true)) {
                    throw new IllegalStateException("injected crash before commit");
                }
            });
        Instant now = Instant.now();
        TaskResultMessage result = new TaskResultMessage(UUID.randomUUID(), 1, taskId, execution, 1, "w-crash",
            AttemptOutcome.SUCCEEDED, mapper.createObjectNode().put("ok", true), null, null, false, now, now, "c");

        assertThatThrownBy(() -> crashing.process(result)).hasMessageContaining("injected crash");
        assertThat(task(execution)).containsEntry("status", "RUNNING");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_attempts WHERE task_execution_id = ?",
            Integer.class, taskId)).isZero();

        assertThat(crashing.process(result)).isEqualTo(ResultProcessor.Outcome.APPLIED);
        assertThat(crashing.process(result)).isEqualTo(ResultProcessor.Outcome.STALE);
        assertThat(task(execution)).containsEntry("status", "SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_attempts WHERE task_execution_id = ?",
            Integer.class, taskId)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_events WHERE task_execution_id = ? "
            + "AND type = 'TaskSucceeded'", Integer.class, taskId)).isEqualTo(1);
    }

    @Test
    @DisplayName("T16.4 Redis paused → the monitor marks nobody OFFLINE and monitor.skipped increments; Redis back → a "
        + "worker that heartbeats again stays ACTIVE (its key expired during the outage), the silent one goes OFFLINE")
    void redisDownSkipsMonitor() {
        String silent = worker("ACTIVE", Instant.now().minusSeconds(3600), "DELAY");
        String beating = worker("ACTIVE", Instant.now().minusSeconds(3600), "DELAY");
        StringRedisTemplate redis = redisTemplate;
        redis.opsForValue().set(RedisKeys.heartbeat(beating), "{\"load\":0}", Duration.ofMillis(1_500));
        double skippedBefore = meters.counter("monitor.skipped").count();
        // Pauses the Redis container every IT shares: safe only because ITs run one at a time.
        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            assertThat(monitor.monitorOnce()).isZero();
            await().pollDelay(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(3)).until(() -> true);   // key expires
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }
        assertThat(meters.counter("monitor.skipped").count()).isGreaterThanOrEqualTo(skippedBefore + 1);

        // Redis is back but the beating worker hasn't rewritten its (expired) key yet: no one is declared dead.
        monitor.monitorOnce();
        assertThat(workers.find(beating).orElseThrow().status()).isEqualTo("ACTIVE");
        assertThat(workers.find(silent).orElseThrow().status()).isEqualTo("ACTIVE");
        redis.opsForValue().set(RedisKeys.heartbeat(beating), "{\"load\":0}", Duration.ofSeconds(30));

        await().atMost(Duration.ofSeconds(20)).until(() -> {
            monitor.monitorOnce();
            return "OFFLINE".equals(workers.find(silent).orElseThrow().status());
        });
        assertThat(workers.find(beating).orElseThrow().status()).isEqualTo("ACTIVE");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE message_key = ? AND message_type = "
            + "'WorkerOffline'", Integer.class, silent)).isEqualTo(1);
        redis.delete(RedisKeys.heartbeat(beating));
    }

    @Test
    @DisplayName("T17.extra-2 attempts still RUNNING on a worker that went OFFLINE long ago (recovery was cut short) "
        + "are recovered by the next monitor pass")
    void interruptedRecoveryIsResumed() {
        String gone = worker("OFFLINE", Instant.now().minusSeconds(3600), "DELAY");
        jdbc.update("UPDATE workers SET offline_at = now() - interval '1 hour' WHERE id = ?", gone);
        UUID execution = runningOn(workflow(), gone, 1);

        monitor.monitorOnce();

        assertThat(task(execution)).containsEntry("status", "RETRYING");
        assertThat(jdbc.queryForObject("SELECT outcome FROM task_attempts WHERE task_execution_id = ?", String.class,
            task(execution).get("id"))).isEqualTo("WORKER_LOST");
    }

    @Test
    @DisplayName("T16.8 capacity counts only ACTIVE workers: DRAINING and OFFLINE ones add nothing")
    void capacityFromActiveWorkersOnly() {
        int before = workers.activeCapacityByType().getOrDefault(TaskType.REPORT, 0);
        worker("ACTIVE", Instant.now(), "REPORT", "DELAY");
        worker("DRAINING", Instant.now(), "REPORT");
        worker("OFFLINE", Instant.now(), "REPORT");

        assertThat(workers.activeCapacityByType().getOrDefault(TaskType.REPORT, 0)).isEqualTo(before + 4);
    }
}

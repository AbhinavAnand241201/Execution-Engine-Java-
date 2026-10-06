package com.flowforge.orchestrator.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.orchestrator.testing.Runs;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The engine's races, driven by hand: the background dispatch loop is off (test profile), results are applied by test
 * threads with a start gate, and dispatch is called directly.
 */
class OrchestrationRacesIT extends OrchestratorIntegrationTestBase {

    private static final String WORKER = "worker-test";
    private static String token;

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private ResultProcessor results;
    @Autowired
    private DispatchService dispatch;
    @Autowired
    private EngineJdbcRepository engine;
    @Autowired
    private EventPublisher publisher;
    @Autowired
    private TransactionTemplate transaction;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private MeterRegistry meters;

    private Runs runs;
    private String diamondWorkflow;
    private final List<String> created = new ArrayList<>();
    /**
     * Commands taken by the hand-built dispatchers below, which record instead of running. Dispatch picks READY tasks
     * of every execution in the shared database, so these may belong to other test classes; cleanUp cancels them all.
     */
    private final Queue<TaskCommand> captured = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setUp() {
        token = token == null ? api().newUserToken() : token;
        runs = new Runs(api(), token);
        diamondWorkflow = runs.publish(Definitions.diamond());
    }

    /** Nothing this class leaves QUEUED/READY/RUNNING may count against capacity in other tests. */
    @AfterEach
    void cleanUp() {
        captured.forEach(command -> created.add(command.workflowExecutionId().toString()));
        for (String id : created) {
            jdbc.update("UPDATE task_executions SET status = 'CANCELLED' WHERE workflow_execution_id = ?::uuid "
                + "AND status IN ('PENDING', 'READY', 'QUEUED', 'RUNNING')", id);
            jdbc.update("UPDATE workflow_executions SET status = 'CANCELLED' WHERE id = ?::uuid "
                + "AND status NOT IN ('COMPLETED', 'FAILED')", id);
        }
    }

    /** A diamond execution with a SUCCEEDED and b, c, d RUNNING (attempt 1) on {@link #WORKER}. */
    private UUID diamondWithMiddleRunning() {
        String id = runs.execute(diamondWorkflow, null);
        created.add(id);
        jdbc.update("UPDATE task_executions SET status = 'SUCCEEDED', attempt = 1 WHERE workflow_execution_id = ?::uuid "
            + "AND task_key = 'a'", id);
        jdbc.update("UPDATE task_executions SET status = 'RUNNING', attempt = 1, worker_id = ?, started_at = now() "
            + "WHERE workflow_execution_id = ?::uuid AND task_key IN ('b', 'c', 'd')", WORKER, id);
        return UUID.fromString(id);
    }

    private UUID taskId(UUID execution, String key) {
        return jdbc.queryForObject("SELECT id FROM task_executions WHERE workflow_execution_id = ? AND task_key = ?",
            UUID.class, execution, key);
    }

    private String status(UUID execution, String key) {
        return jdbc.queryForObject("SELECT status FROM task_executions WHERE workflow_execution_id = ? AND task_key = ?",
            String.class, execution, key);
    }

    private int readyEvents(UUID execution, String key) {
        return jdbc.queryForObject("SELECT count(*) FROM workflow_events e JOIN task_executions t "
            + "ON t.id = e.task_execution_id WHERE e.workflow_execution_id = ? AND t.task_key = ? AND e.type = 'TaskReady'",
            Integer.class, execution, key);
    }

    private TaskResultMessage success(UUID execution, String key, int attempt) {
        Instant now = Instant.now();
        return new TaskResultMessage(UUID.randomUUID(), 1, taskId(execution, key), execution, attempt, WORKER,
            AttemptOutcome.SUCCEEDED, mapper.createObjectNode().put("key", key), null, null, false, now, now, "corr");
    }

    @Test
    @DisplayName("T12.3 join race: results for b, c, d applied by 3 threads (start gate, 50 iterations) → e READY "
        + "exactly once and dispatched exactly once (attempt 1)")
    void joinRace() throws Exception {
        List<UUID> executionIds = new ArrayList<>();
        List<List<TaskResultMessage>> messages = new ArrayList<>();
        List<List<Object>> outcomes = ConcurrencyHarness.race(3, 50,
            thread -> results.process(messages.getLast().get(thread)),
            () -> {
                UUID execution = diamondWithMiddleRunning();
                executionIds.add(execution);
                messages.add(List.of(success(execution, "b", 1), success(execution, "c", 1), success(execution, "d", 1)));
            });

        for (int i = 0; i < 50; i++) {
            UUID execution = executionIds.get(i);
            assertThat(outcomes.get(i)).as("iteration %d", i).containsOnly(ResultProcessor.Outcome.APPLIED);
            assertThat(status(execution, "e")).as("iteration %d", i).isEqualTo("READY");
            assertThat(readyEvents(execution, "e")).as("iteration %d", i).isEqualTo(1);
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> {
            dispatch.dispatchOnce();
            return executionIds.stream().allMatch(id -> "COMPLETED".equals(jdbc.queryForObject(
                "SELECT status FROM workflow_executions WHERE id = ?", String.class, id)));
        });
        for (UUID execution : executionIds) {
            assertThat(jdbc.queryForObject("SELECT attempt FROM task_executions WHERE workflow_execution_id = ? "
                + "AND task_key = 'e'", Integer.class, execution)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("T12.4 a result with a stale attempt is ignored: no change, task.results.stale incremented")
    void staleAttempt() {
        UUID execution = diamondWithMiddleRunning();
        double before = meters.counter("task.results.stale").count();

        assertThat(results.process(success(execution, "b", 2))).isEqualTo(ResultProcessor.Outcome.STALE);

        assertThat(status(execution, "b")).isEqualTo("RUNNING");
        assertThat(meters.counter("task.results.stale").count()).isEqualTo(before + 1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_attempts WHERE task_execution_id = ?", Integer.class,
            taskId(execution, "b"))).isZero();
    }

    @Test
    @DisplayName("T12.5 the same result delivered twice → the second is a no-op; e becomes READY once")
    void duplicateResult() {
        UUID execution = diamondWithMiddleRunning();
        results.process(success(execution, "c", 1));
        results.process(success(execution, "d", 1));
        TaskResultMessage b = success(execution, "b", 1);

        assertThat(results.process(b)).isEqualTo(ResultProcessor.Outcome.APPLIED);
        assertThat(results.process(b)).isEqualTo(ResultProcessor.Outcome.STALE);

        assertThat(readyEvents(execution, "e")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_attempts WHERE task_execution_id = ?", Integer.class,
            taskId(execution, "b"))).isEqualTo(1);
    }

    @Test
    @DisplayName("T12.7 capacity 1 with LOW and CRITICAL READY tasks in two executions → CRITICAL dispatched first")
    void priority() {
        String low = runs.execute(runs.publish(Map.of("tasks", List.of(
            Runs.task("slow-lane", "DELAY", Map.of("durationMs", 0), "LOW")))), null);
        String critical = runs.execute(runs.publish(Map.of("tasks", List.of(
            Runs.task("fast-lane", "DELAY", Map.of("durationMs", 0), "CRITICAL")))), null);
        created.add(low);
        created.add(critical);
        Queue<TaskCommand> sent = captured;
        CapacityProvider one = () -> {
            int[] left = {1};
            return type -> left[0]-- > 0;
        };
        DispatchService capacityOne = new DispatchService(engine, publisher, sent::add, one, transaction,
            Clock.systemUTC(), mapper, 100);

        assertThat(capacityOne.dispatchOnce()).isEqualTo(1);

        assertThat(sent).singleElement().satisfies(command -> {
            assertThat(command.taskKey()).isEqualTo("fast-lane");
            assertThat(command.workflowExecutionId()).hasToString(critical);
            assertThat(command.attempt()).isEqualTo(1);
        });
        assertThat(jdbc.queryForObject("SELECT status FROM task_executions WHERE workflow_execution_id = ?::uuid",
            String.class, low)).isEqualTo("READY");
    }

    @Test
    @DisplayName("T12.11 results + dispatch + a cancel on one execution, concurrently × 100 → no deadlock (40P01), "
        + "consistent state")
    void noDeadlock() throws Exception {
        Queue<TaskCommand> sent = captured;
        DispatchService capturing = new DispatchService(engine, publisher, sent::add, () -> type -> true, transaction,
            Clock.systemUTC(), mapper, 100);
        List<UUID> executionIds = new ArrayList<>();
        List<List<TaskResultMessage>> messages = new ArrayList<>();

        List<List<Object>> outcomes = ConcurrencyHarness.race(5, 100, thread -> switch (thread) {
            case 0, 1, 2 -> results.process(messages.getLast().get(thread));
            case 3 -> capturing.dispatchOnce();
            default -> stubCancel(executionIds.getLast());
        }, () -> {
            UUID execution = diamondWithMiddleRunning();
            executionIds.add(execution);
            messages.add(List.of(success(execution, "b", 1), success(execution, "c", 1), success(execution, "d", 1)));
        });

        for (int i = 0; i < 100; i++) {
            UUID execution = executionIds.get(i);
            for (Object outcome : outcomes.get(i)) {
                assertThat(outcome).as("iteration %d", i).isNotInstanceOf(Throwable.class);
            }
            for (String key : List.of("b", "c", "d")) {
                assertThat(status(execution, key)).isEqualTo("SUCCEEDED");
            }
            assertThat(readyEvents(execution, "e")).isLessThanOrEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT attempt FROM task_executions WHERE workflow_execution_id = ? "
                + "AND task_key = 'e'", Integer.class, execution)).isLessThanOrEqualTo(1);
            String workflowStatus = jdbc.queryForObject("SELECT status FROM workflow_executions WHERE id = ?",
                String.class, execution);
            assertThat(workflowStatus).isIn("RUNNING", "CANCELLING", "CANCELLED");
            if (!workflowStatus.equals("RUNNING")) {
                assertThat(status(execution, "e")).isIn("PENDING", "READY", "QUEUED");
            }
        }
    }

    @Test
    @DisplayName("T12.extra-1 a success after the workflow FAILED still makes its dependents READY, "
        + "but they are not dispatched and the workflow stays FAILED")
    void successAfterFailure() {
        String id = runs.execute(runs.publish(Map.of("tasks", List.of(Definitions.delay("x", 0),
            Definitions.delay("y", 0, "x"), Definitions.delay("z", 0)))), null);
        created.add(id);
        UUID execution = UUID.fromString(id);
        jdbc.update("UPDATE task_executions SET status = 'RUNNING', attempt = 1, worker_id = ?, started_at = now() "
            + "WHERE workflow_execution_id = ? AND task_key IN ('x', 'z')", WORKER, execution);
        Instant now = Instant.now();
        TaskResultMessage zFailed = new TaskResultMessage(UUID.randomUUID(), 1, taskId(execution, "z"), execution, 1,
            WORKER, AttemptOutcome.FAILED, null, "BOOM", "permanent", false, now, now, "corr");

        results.process(zFailed);
        assertThat(results.process(success(execution, "x", 1))).isEqualTo(ResultProcessor.Outcome.APPLIED);

        assertThat(jdbc.queryForObject("SELECT status FROM workflow_executions WHERE id = ?", String.class, execution))
            .isEqualTo("FAILED");
        assertThat(status(execution, "y")).isEqualTo("READY");
        assertThat(readyEvents(execution, "y")).isEqualTo(1);
        Queue<TaskCommand> sent = captured;
        new DispatchService(engine, publisher, sent::add, () -> type -> true, transaction, Clock.systemUTC(), mapper,
            100).dispatchOnce();
        assertThat(sent).noneMatch(command -> command.workflowExecutionId().equals(execution));
        assertThat(status(execution, "y")).isEqualTo("READY");
    }

    /** A stand-in for a cancel: workflow row lock first, then the task rows. */
    private Object stubCancel(UUID execution) {
        return transaction.execute(status -> {
            var locked = engine.lockExecution(execution).orElseThrow();
            if (locked.status().equals("RUNNING")) {
                engine.setExecutionStatus(execution, "RUNNING", "CANCELLING", Instant.now(), false);
            }
            jdbc.update("UPDATE task_executions SET updated_at = now() WHERE workflow_execution_id = ?", execution);
            return "cancelled";
        });
    }
}

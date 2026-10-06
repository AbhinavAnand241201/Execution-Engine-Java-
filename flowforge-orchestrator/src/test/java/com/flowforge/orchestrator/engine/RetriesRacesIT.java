package com.flowforge.orchestrator.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.core.testing.MutableClock;
import com.flowforge.core.error.ErrorCode;
import com.flowforge.orchestrator.application.ApplicationException;
import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.DeadLetterService;
import com.flowforge.orchestrator.application.Role;
import com.flowforge.orchestrator.testing.ApiClient;
import org.springframework.http.HttpMethod;
import com.flowforge.orchestrator.application.PublishedDagCache;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.orchestrator.testing.Runs;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Retry timing, timeouts, the DLQ list and concurrent promoters, driven by hand: background jobs are off (test
 * profile) and the promoter / sweeper / result processor are built with a {@link MutableClock} where time matters.
 */
class RetriesRacesIT extends OrchestratorIntegrationTestBase {

    private static final String WORKER = "worker-test";
    private static String token;

    @Autowired
    private JdbcTemplate jdbc;
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
    private DeadLetterService deadLetters;

    private Runs runs;
    private final List<String> created = new ArrayList<>();
    /** Commands taken by the recording dispatcher; may include other classes' tasks, so cleanUp cancels them too. */
    private final Queue<TaskCommand> captured = new ConcurrentLinkedQueue<>();

    @BeforeEach
    void setUp() {
        token = token == null ? api().newUserToken() : token;
        runs = new Runs(api(), token);
    }

    @AfterEach
    void cleanUp() {
        captured.forEach(command -> created.add(command.workflowExecutionId().toString()));
        for (String id : created) {
            jdbc.update("UPDATE task_executions SET status = 'CANCELLED' WHERE workflow_execution_id = ?::uuid "
                + "AND status IN ('PENDING', 'READY', 'QUEUED', 'RUNNING', 'RETRYING')", id);
            jdbc.update("UPDATE workflow_executions SET status = 'CANCELLED' WHERE id = ?::uuid "
                + "AND status NOT IN ('COMPLETED', 'FAILED')", id);
        }
    }

    /** A published single-task workflow whose retries wait exactly 1 s (FIXED, no jitter), up to 3 attempts. */
    private String oneTaskWorkflow() {
        Map<String, Object> task = new HashMap<>(Runs.task("only", "DELAY", Map.of("durationMs", 0), null));
        task.put("retry", Map.of("maxAttempts", 3, "backoff", "FIXED", "initialDelayMs", 1000, "maxDelayMs", 1000,
            "jitter", false));
        return runs.publish(Map.of("tasks", List.of(task)));
    }

    /** A new execution of {@code workflowId} whose task is RUNNING attempt 1 on {@link #WORKER}, deadline as given. */
    private UUID running(String workflowId, Instant deadline) {
        String id = runs.execute(workflowId, null);
        created.add(id);
        jdbc.update("UPDATE task_executions SET status = 'RUNNING', attempt = 1, worker_id = ?, started_at = ?, "
            + "deadline_at = ? WHERE workflow_execution_id = ?::uuid", WORKER, Timestamp.from(Instant.now()),
            deadline == null ? null : Timestamp.from(deadline), id);
        return UUID.fromString(id);
    }

    private UUID taskId(UUID execution) {
        return jdbc.queryForObject("SELECT id FROM task_executions WHERE workflow_execution_id = ?", UUID.class,
            execution);
    }

    private Map<String, Object> task(UUID execution) {
        return jdbc.queryForMap("SELECT status, attempt, retry_count, next_attempt_at, ready_at FROM task_executions "
            + "WHERE workflow_execution_id = ?", execution);
    }

    private TaskResultMessage result(UUID execution, AttemptOutcome outcome, String errorCode, boolean retryable) {
        Instant now = Instant.now();
        return new TaskResultMessage(UUID.randomUUID(), 1, taskId(execution), execution, 1, WORKER, outcome,
            outcome == AttemptOutcome.SUCCEEDED ? mapper.createObjectNode() : null, errorCode,
            errorCode == null ? null : "test failure", retryable, now, now, "corr");
    }

    private ResultProcessor processorAt(Clock clock) {
        return new ResultProcessor(engine, publisher, dags, failures, transaction, clock, mapper, readyTasks,
            new SimpleMeterRegistry());
    }

    private RetryPromoter promoterAt(Clock clock) {
        return new RetryPromoter(engine, publisher, transaction, clock, readyTasks, mapper, 100);
    }

    @Test
    @DisplayName("T13.2 backoff is respected (MutableClock): the promoter leaves the task RETRYING before "
        + "next_attempt_at and makes it READY once it is due")
    void backoffRespected() {
        MutableClock clock = new MutableClock(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        Instant failedAt = clock.instant();
        UUID execution = running(oneTaskWorkflow(), null);

        processorAt(clock).process(result(execution, AttemptOutcome.FAILED, "FLAKY", true));
        assertThat(task(execution)).containsEntry("status", "RETRYING").containsEntry("retry_count", 1);
        assertThat(((Timestamp) task(execution).get("next_attempt_at")).toInstant())
            .isEqualTo(failedAt.plusSeconds(1));

        RetryPromoter promoter = promoterAt(clock);
        clock.advance(Duration.ofMillis(999));
        promoter.promoteDue();
        assertThat(task(execution)).containsEntry("status", "RETRYING");

        clock.advance(Duration.ofMillis(1));
        promoter.promoteDue();
        assertThat(task(execution)).containsEntry("status", "READY").containsEntry("next_attempt_at", null);
        assertThat(((Timestamp) task(execution).get("ready_at")).toInstant()).isEqualTo(failedAt.plusSeconds(1));
    }

    @Test
    @DisplayName("T13.4 timeout sweeper: RUNNING past deadline + grace → attempt TIMED_OUT, task RETRYING; a late "
        + "success for that attempt is ignored")
    void timeoutSweeper() {
        Instant now = Instant.now();
        String workflowId = oneTaskWorkflow();
        UUID overdue = running(workflowId, now.minusSeconds(1));
        UUID withinGrace = running(workflowId, now.minusMillis(100));
        TimeoutSweeper sweeper = new TimeoutSweeper(engine, publisher, failures, transaction,
            Clock.fixed(now, java.time.ZoneOffset.UTC), mapper, Duration.ofMillis(200), 100);

        sweeper.sweep();

        assertThat(task(overdue)).containsEntry("status", "RETRYING").containsEntry("retry_count", 1);
        assertThat(jdbc.queryForObject("SELECT outcome FROM task_attempts WHERE task_execution_id = ? AND attempt = 1",
            String.class, taskId(overdue))).isEqualTo("TIMED_OUT");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_events WHERE workflow_execution_id = ? "
            + "AND type = 'TaskTimedOut'", Integer.class, overdue)).isEqualTo(1);
        assertThat(task(withinGrace)).containsEntry("status", "RUNNING");

        assertThat(results.process(result(overdue, AttemptOutcome.SUCCEEDED, null, false)))
            .isEqualTo(ResultProcessor.Outcome.STALE);
        assertThat(task(overdue)).containsEntry("status", "RETRYING");
    }

    @Test
    @DisplayName("T13.5 DLQ list: filters by workflowId and errorCode, pages; a USER gets 403")
    void deadLetterList() {
        String first = oneTaskWorkflow();
        String second = oneTaskWorkflow();
        results.process(result(running(first, null), AttemptOutcome.FAILED, "BOOM_A", false));
        results.process(result(running(first, null), AttemptOutcome.FAILED, "BOOM_A", false));
        results.process(result(running(second, null), AttemptOutcome.FAILED, "BOOM_B", false));
        String operator = api().operatorToken();

        JsonNode all = api().read(api().get("/api/v1/dead-letter/tasks?workflowId=" + first, operator));
        assertThat(all.get("totalElements").asInt()).isEqualTo(2);
        assertThat(all.get("items")).allSatisfy(e -> assertThat(e.get("errorCode").asText()).isEqualTo("BOOM_A"));

        JsonNode page = api().read(api().get("/api/v1/dead-letter/tasks?workflowId=" + first + "&size=1&page=1",
            operator));
        assertThat(page.get("items")).hasSize(1);
        assertThat(page.get("totalPages").asInt()).isEqualTo(2);

        JsonNode byCode = api().read(api().get("/api/v1/dead-letter/tasks?workflowId=" + second + "&errorCode=BOOM_B",
            operator));
        assertThat(byCode.get("items")).singleElement()
            .satisfies(e -> assertThat(e.get("workflowId").asText()).isEqualTo(second));
        assertThat(api().read(api().get("/api/v1/dead-letter/tasks?workflowId=" + second + "&errorCode=BOOM_A",
            operator)).get("items")).isEmpty();

        assertThat(api().get("/api/v1/dead-letter/tasks", token).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
    }

    @Test
    @DisplayName("T13.8 two promoters at once (start gate, 50 iterations) → each task promoted once; the next "
        + "dispatch increments attempt exactly once")
    void concurrentPromoters() throws Exception {
        String workflowId = oneTaskWorkflow();
        List<UUID> executionIds = new ArrayList<>();
        RetryPromoter[] promoters = {promoterAt(Clock.systemUTC()), promoterAt(Clock.systemUTC())};

        ConcurrencyHarness.race(2, 50, thread -> promoters[thread].promoteDue(), () -> {
            UUID execution = running(workflowId, null);
            jdbc.update("UPDATE task_executions SET status = 'RETRYING', retry_count = 1, next_attempt_at = ? "
                + "WHERE workflow_execution_id = ?", Timestamp.from(Instant.now().minusSeconds(1)), execution);
            executionIds.add(execution);
        });

        for (UUID execution : executionIds) {
            assertThat(task(execution)).containsEntry("status", "READY");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_events WHERE workflow_execution_id = ? "
                + "AND type = 'TaskReady' AND data->>'retry' = 'true'", Integer.class, execution)).isEqualTo(1);
        }
        DispatchService recording = new DispatchService(engine, publisher, captured::add, () -> type -> true,
            transaction, Clock.systemUTC(), mapper, 500);
        recording.dispatchOnce();
        for (UUID execution : executionIds) {
            assertThat(task(execution)).containsEntry("status", "QUEUED").containsEntry("attempt", 2);
            assertThat(captured).filteredOn(c -> c.workflowExecutionId().equals(execution)).singleElement()
                .satisfies(c -> assertThat(c.attempt()).isEqualTo(2));
        }
    }

    private AuthenticatedUser operatorUser() {
        UUID id = jdbc.queryForObject("SELECT id FROM users WHERE email = ?", UUID.class, ApiClient.OPERATOR_EMAIL);
        return new AuthenticatedUser(id, ApiClient.OPERATOR_EMAIL, Role.OPERATOR);
    }

    @Test
    @DisplayName("T13.extra-1 two replays of one entry at once (start gate, 50 iterations) → exactly one succeeds, "
        + "the other gets 409 ALREADY_REPLAYED; the task is made READY once")
    void concurrentReplays() throws Exception {
        String workflowId = oneTaskWorkflow();
        AuthenticatedUser operator = operatorUser();
        List<UUID> executionIds = new ArrayList<>();
        List<UUID> entries = new ArrayList<>();

        List<List<Object>> outcomes = ConcurrencyHarness.race(2, 50, thread -> deadLetters.replay(operator,
            entries.getLast()), () -> {
                UUID execution = running(workflowId, null);
                results.process(result(execution, AttemptOutcome.FAILED, "BOOM", false));
                executionIds.add(execution);
                entries.add(jdbc.queryForObject("SELECT id FROM dead_letter_entries WHERE workflow_execution_id = ?",
                    UUID.class, execution));
            });

        for (int i = 0; i < 50; i++) {
            assertThat(outcomes.get(i)).as("iteration %d", i)
                .filteredOn(o -> o instanceof DeadLetterService.ReplayView).hasSize(1);
            assertThat(outcomes.get(i)).as("iteration %d", i).filteredOn(o -> o instanceof ApplicationException e
                && e.errorCode() == ErrorCode.ALREADY_REPLAYED).hasSize(1);
            UUID execution = executionIds.get(i);
            assertThat(task(execution)).containsEntry("status", "READY").containsEntry("retry_count", 0)
                .containsEntry("attempt", 1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_events WHERE workflow_execution_id = ? "
                + "AND type = 'WorkflowRetried'", Integer.class, execution)).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("T13.extra-2 a timeout in a CANCELLING execution → the task becomes CANCELLED (no retry) and the "
        + "execution CANCELLED")
    void timeoutWhileCancelling() {
        Instant now = Instant.now();
        UUID execution = running(oneTaskWorkflow(), now.minusSeconds(5));
        jdbc.update("UPDATE workflow_executions SET status = 'CANCELLING' WHERE id = ?", execution);

        new TimeoutSweeper(engine, publisher, failures, transaction, Clock.fixed(now, java.time.ZoneOffset.UTC), mapper,
            Duration.ofMillis(200), 100).sweep();

        assertThat(task(execution)).containsEntry("status", "CANCELLED").containsEntry("retry_count", 0);
        assertThat(jdbc.queryForObject("SELECT status FROM workflow_executions WHERE id = ?", String.class, execution))
            .isEqualTo("CANCELLED");
    }

    @Test
    @DisplayName("T13.extra-3 access: execution retry by another USER → 404, by an OPERATOR who isn't the owner → "
        + "403; DLQ replay by a USER → 403")
    void replayAccess() {
        UUID execution = running(oneTaskWorkflow(), null);
        results.process(result(execution, AttemptOutcome.FAILED, "BOOM", false));
        UUID entry = jdbc.queryForObject("SELECT id FROM dead_letter_entries WHERE workflow_execution_id = ?",
            UUID.class, execution);
        String path = "/api/v1/executions/" + execution + "/retry";

        assertThat(api().send(HttpMethod.POST, path, null, api().newUserToken()).getStatusCode())
            .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api().send(HttpMethod.POST, path, null, api().operatorToken()).getStatusCode())
            .isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(api().send(HttpMethod.POST, "/api/v1/dead-letter/tasks/" + entry + "/replay", null, token)
            .getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(task(execution)).containsEntry("status", "FAILED_PERMANENTLY");
    }

    @Test
    @DisplayName("T13.extra-4 queue reconciler: a task QUEUED for longer than stale-queued-after gets its command "
        + "again, same attempt, new message id; a fresh QUEUED task is left alone")
    void reconcilerResendsStaleCommands() {
        Instant now = Instant.now();
        String workflowId = oneTaskWorkflow();
        String staleId = runs.execute(workflowId, null);
        String freshId = runs.execute(workflowId, null);
        created.add(staleId);
        created.add(freshId);
        jdbc.update("UPDATE task_executions SET status = 'QUEUED', attempt = 1, queued_at = ? "
            + "WHERE workflow_execution_id = ?::uuid", Timestamp.from(now.minus(Duration.ofDays(3650))), staleId);
        jdbc.update("UPDATE task_executions SET status = 'QUEUED', attempt = 1, queued_at = ? "
            + "WHERE workflow_execution_id = ?::uuid", Timestamp.from(now), freshId);
        DispatchService recording = new DispatchService(engine, publisher, captured::add, () -> type -> true,
            transaction, Clock.systemUTC(), mapper, 100);

        new QueueReconciler(engine, recording, Clock.fixed(now, java.time.ZoneOffset.UTC), Duration.ofSeconds(60), 100,
            new SimpleMeterRegistry()).reconcile();

        assertThat(captured).filteredOn(c -> c.workflowExecutionId().toString().equals(staleId)).singleElement()
            .satisfies(c -> assertThat(c.attempt()).isEqualTo(1));
        assertThat(captured).noneMatch(c -> c.workflowExecutionId().toString().equals(freshId));
        assertThat(task(UUID.fromString(staleId))).containsEntry("status", "QUEUED").containsEntry("attempt", 1);
    }
}

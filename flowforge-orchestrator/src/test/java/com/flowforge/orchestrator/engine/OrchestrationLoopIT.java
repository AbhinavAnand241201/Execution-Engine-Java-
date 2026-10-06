package com.flowforge.orchestrator.engine;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.orchestrator.testing.Runs;
import com.flowforge.runtime.runner.TaskRunner;
import com.github.tomakehurst.wiremock.WireMockServer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * The whole loop in local mode: execute → dispatch → claim → run → result → next tasks. The dispatch loop runs only in
 * this class's context, which is closed afterwards so it can't pick up other tests' READY tasks.
 */
@TestPropertySource(properties = "flowforge.dispatch.job-enabled=true")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrchestrationLoopIT extends OrchestratorIntegrationTestBase {

    private static WireMockServer wiremock;
    private static String token;

    @Autowired
    private JdbcTemplate jdbc;

    @Autowired
    private TaskRunner runner;

    private Runs runs;

    @BeforeAll
    static void startWireMock() {
        wiremock = new WireMockServer(options().dynamicPort());
        wiremock.start();
    }

    @AfterAll
    static void stopWireMock() {
        wiremock.stop();
    }

    @BeforeEach
    void setUp() {
        token = token == null ? api().newUserToken() : token;
        runs = new Runs(api(), token);
        wiremock.resetAll();
    }

    private static Instant at(JsonNode task, String field) {
        return Instant.parse(task.get(field).asText());
    }

    @Test
    @DisplayName("T12.1 a linear 3-task DELAY workflow → COMPLETED; tasks SUCCEEDED in dependency order; full timeline")
    void linear() {
        String id = runs.run(Definitions.linear(3));

        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(10), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(execution.get("completedAt")).isNotNull();
        JsonNode t0 = Runs.task(execution, "t0");
        JsonNode t1 = Runs.task(execution, "t1");
        JsonNode t2 = Runs.task(execution, "t2");
        for (JsonNode task : List.of(t0, t1, t2)) {
            assertThat(task.get("status").asText()).isEqualTo("SUCCEEDED");
            assertThat(task.get("attempt").asInt()).isEqualTo(1);
        }
        assertThat(at(t1, "startedAt")).isAfterOrEqualTo(at(t0, "completedAt"));
        assertThat(at(t2, "startedAt")).isAfterOrEqualTo(at(t1, "completedAt"));

        List<String> types = runs.events(id).stream().map(e -> e.get("type").asText()).toList();
        assertThat(types.getFirst()).isEqualTo("WorkflowStarted");
        assertThat(types.getLast()).isEqualTo("WorkflowCompleted");
        assertThat(types).filteredOn("TaskReady"::equals).hasSize(3);
        assertThat(types).filteredOn("TaskQueued"::equals).hasSize(3);
        assertThat(types).filteredOn("TaskSucceeded"::equals).hasSize(3);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM task_attempts a JOIN task_executions t "
            + "ON t.id = a.task_execution_id WHERE t.workflow_execution_id = ?::uuid AND a.outcome = 'SUCCEEDED'",
            Integer.class, id)).isEqualTo(3);
    }

    @Test
    @DisplayName("T12.2 diamond with 500 ms DELAYs on b, c, d → they overlap; total < 1.4 s; e starts after all three")
    void diamondParallelism() {
        Map<String, Object> diamond = Map.of("tasks", List.of(Definitions.delay("a", 0), Definitions.delay("b", 500, "a"),
            Definitions.delay("c", 500, "a"), Definitions.delay("d", 500, "a"), Definitions.delay("e", 0, "b", "c", "d")));
        String id = runs.run(diamond);

        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(10), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        List<JsonNode> middle = List.of(Runs.task(execution, "b"), Runs.task(execution, "c"), Runs.task(execution, "d"));
        Instant lastStart = middle.stream().map(t -> at(t, "startedAt")).max(Instant::compareTo).orElseThrow();
        Instant firstEnd = middle.stream().map(t -> at(t, "completedAt")).min(Instant::compareTo).orElseThrow();
        Instant lastEnd = middle.stream().map(t -> at(t, "completedAt")).max(Instant::compareTo).orElseThrow();
        assertThat(lastStart).as("all three running at once").isBefore(firstEnd);
        assertThat(at(Runs.task(execution, "e"), "startedAt")).isAfterOrEqualTo(lastEnd);
        assertThat(Duration.between(at(execution, "createdAt"), at(execution, "completedAt")))
            .isLessThan(Duration.ofMillis(1_400));
    }

    @Test
    @DisplayName("T12.6 a non-retryable failure → task FAILED_PERMANENTLY, workflow FAILED, dependents stay PENDING")
    void permanentFailure() {
        wiremock.stubFor(post("/reject").willReturn(aResponse().withStatus(400)));
        Map<String, Object> definition = Map.of("tasks", List.of(
            Runs.task("call", "HTTP", Map.of("method", "POST", "url", "http://localhost:" + wiremock.port() + "/reject"),
                null),
            Definitions.delay("after", 0, "call")));
        String id = runs.run(definition);

        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(10), "FAILED", "COMPLETED");
        assertThat(execution.get("status").asText()).isEqualTo("FAILED");
        JsonNode call = Runs.task(execution, "call");
        assertThat(call.get("status").asText()).isEqualTo("FAILED_PERMANENTLY");
        assertThat(call.get("errorCode").asText()).isEqualTo("HTTP_STATUS");
        assertThat(Runs.task(execution, "after").get("status").asText()).isEqualTo("PENDING");
        assertThat(Runs.task(execution, "after").get("attempt").asInt()).isZero();
        wiremock.verify(1, postRequestedFor(urlEqualTo("/reject")));
    }

    @Test
    @DisplayName("T12.8 in-flight (QUEUED + RUNNING) never exceeds capacity (8) during a 50-task run")
    void capacityRespected() throws Exception {
        List<Object> tasks = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            tasks.add(Definitions.delay("t" + i, 30));
        }
        String id = runs.run(Map.of("tasks", tasks));

        AtomicInteger max = new AtomicInteger();
        AtomicBoolean done = new AtomicBoolean();
        Thread sampler = Thread.ofPlatform().start(() -> {
            while (!done.get()) {
                Integer inFlight = jdbc.queryForObject("SELECT count(*) FROM task_executions "
                    + "WHERE workflow_execution_id = ?::uuid AND status IN ('QUEUED', 'RUNNING')", Integer.class, id);
                max.accumulateAndGet(inFlight, Math::max);
                Thread.onSpinWait();
            }
        });
        try {
            runs.awaitStatus(id, Duration.ofSeconds(30), "COMPLETED", "FAILED");
        } finally {
            done.set(true);
            sampler.join();
        }
        assertThat(runs.execution(id).get("status").asText()).isEqualTo("COMPLETED");
        assertThat(max.get()).isPositive().isLessThanOrEqualTo(8);
    }

    @Test
    @DisplayName("T12.9 monthly report: DATABASE rows → REPORT aggregates them → HTTP notify hits WireMock once")
    void monthlyReport() {
        wiremock.stubFor(post("/notify").willReturn(aResponse().withStatus(204)));
        Map<String, Object> definition = Map.of("tasks", List.of(
            Runs.task("orders", "DATABASE", Map.of("query", "orders_by_status", "params", Map.of("status", "PAID")), null),
            Runs.task("summary", "REPORT", Map.of("source", "orders", "groupBy", "customer",
                "metrics", List.of(Map.of("field", "amount", "fn", "count"), Map.of("field", "amount", "fn", "sum"))),
                null, "orders"),
            Runs.task("notify", "HTTP", Map.of("method", "POST", "url", "http://localhost:" + wiremock.port() + "/notify",
                "body", Map.of("report", "monthly")), null, "summary")));
        String id = runs.run(definition);

        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(15), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        JsonNode summary = Runs.task(execution, "summary").get("output");
        assertThat(summary.get("totals").get("count_amount").asInt()).isEqualTo(125);
        assertThat(summary.get("groups")).isNotEmpty();
        assertThat(Runs.task(execution, "notify").get("output").get("status").asInt()).isEqualTo(204);
        wiremock.verify(1, postRequestedFor(urlEqualTo("/notify")));
    }

    @Test
    @DisplayName("T12.10 dispatch happens after commit: 100 runs, zero spurious claim failures")
    void dispatchAfterCommit() {
        long rejectedBefore = runner.duplicateOrStaleClaims();
        String workflowId = runs.publish(Map.of("tasks", List.of(Definitions.delay("only", 0))));
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            ids.add(runs.execute(workflowId, Map.of("n", i)));
        }
        for (String id : ids) {
            assertThat(runs.awaitStatus(id, Duration.ofSeconds(30), "COMPLETED", "FAILED").get("status").asText())
                .isEqualTo("COMPLETED");
        }
        assertThat(runner.duplicateOrStaleClaims() - rejectedBefore).isZero();
    }
}

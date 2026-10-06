package com.flowforge.orchestrator.engine;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.orchestrator.testing.Runs;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

/**
 * Retries, dead-lettering and replay end to end in local mode: the dispatch loop, retry promoter and timeout sweeper
 * all run, in a context of this class's own (closed afterwards). WireMock plays a flaky downstream service.
 */
@TestPropertySource(properties = {"flowforge.dispatch.job-enabled=true", "flowforge.jobs.enabled=true"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class RetriesLoopIT extends OrchestratorIntegrationTestBase {

    private static WireMockServer wiremock;
    private static String token;
    private static String operator;

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
        operator = operator == null ? api().operatorToken() : operator;
        runs = new Runs(api(), token);
        wiremock.resetAll();
    }

    /** An HTTP task POSTing to {@code path}, with a fast fixed backoff so retries take milliseconds. */
    private Map<String, Object> call(String key, String path, int maxAttempts, String... dependsOn) {
        Map<String, Object> task = new HashMap<>(Runs.task(key, "HTTP",
            Map.of("method", "POST", "url", "http://localhost:" + wiremock.port() + path), null, dependsOn));
        task.put("retry", Map.of("maxAttempts", maxAttempts, "backoff", "FIXED", "initialDelayMs", 50,
            "maxDelayMs", 50, "jitter", false));
        return task;
    }

    private void flaky(String path, int failures) {
        String state = Scenario.STARTED;
        for (int i = 1; i <= failures; i++) {
            wiremock.stubFor(post(path).inScenario(path).whenScenarioStateIs(state)
                .willReturn(aResponse().withStatus(503)).willSetStateTo("failed-" + i));
            state = "failed-" + i;
        }
        wiremock.stubFor(post(path).inScenario(path).whenScenarioStateIs(state).willReturn(aResponse().withStatus(200)));
    }

    private List<String> outcomes(String executionId, String taskKey) {
        JsonNode attempts = api().read(api().get("/api/v1/executions/" + executionId + "/tasks/" + taskKey + "/attempts",
            token));
        return java.util.stream.StreamSupport.stream(attempts.spliterator(), false)
            .map(a -> a.get("attempt").asInt() + ":" + a.get("outcome").asText()).toList();
    }

    private JsonNode deadLetters(String workflowId, boolean includeReplayed) {
        return api().read(api().get("/api/v1/dead-letter/tasks?workflowId=" + workflowId + "&includeReplayed="
            + includeReplayed, operator)).get("items");
    }

    private ResponseEntity<String> replay(String entryId) {
        return api().send(HttpMethod.POST, "/api/v1/dead-letter/tasks/" + entryId + "/replay", null, operator);
    }

    @Test
    @DisplayName("T13.1 a downstream that fails twice (503, 503, 200), maxAttempts 3 → SUCCEEDED; attempts 1–3 recorded "
        + "FAILED, FAILED, SUCCEEDED")
    void flakyThenSucceeds() {
        flaky("/flaky", 2);
        String id = runs.run(Map.of("tasks", List.of(call("call", "/flaky", 3))));

        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(15), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        JsonNode task = Runs.task(execution, "call");
        assertThat(task.get("attempt").asInt()).isEqualTo(3);
        assertThat(task.get("retryCount").asInt()).isEqualTo(2);
        assertThat(outcomes(id, "call")).containsExactly("1:FAILED", "2:FAILED", "3:SUCCEEDED");
        List<String> types = runs.events(id).stream().map(e -> e.get("type").asText()).toList();
        assertThat(types).filteredOn("TaskRetryScheduled"::equals).hasSize(2);
        wiremock.verify(3, postRequestedFor(urlEqualTo("/flaky")));
    }

    @Test
    @DisplayName("T13.3 always failing, maxAttempts 3 → FAILED_PERMANENTLY, one DLQ entry, execution FAILED")
    void alwaysFailing() {
        wiremock.stubFor(post("/down").willReturn(aResponse().withStatus(503)));
        String workflowId = runs.publish(Map.of("tasks", List.of(call("call", "/down", 3))));
        String id = runs.execute(workflowId, null);

        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(15), "FAILED", "COMPLETED");
        assertThat(execution.get("status").asText()).isEqualTo("FAILED");
        assertThat(Runs.task(execution, "call").get("status").asText()).isEqualTo("FAILED_PERMANENTLY");
        assertThat(outcomes(id, "call")).containsExactly("1:FAILED", "2:FAILED", "3:FAILED");
        JsonNode entries = deadLetters(workflowId, false);
        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).get("taskKey").asText()).isEqualTo("call");
        assertThat(entries.get(0).get("attempt").asInt()).isEqualTo(3);
        assertThat(entries.get(0).get("errorCode").asText()).isEqualTo("HTTP_STATUS");
        assertThat(runs.events(id).stream().map(e -> e.get("type").asText()))
            .contains("TaskDeadLettered", "WorkflowFailed");
    }

    @Test
    @DisplayName("T13.6 replay after the downstream recovers → the task runs again → execution COMPLETED; the entry is "
        + "marked replayed by the operator")
    void replayCompletes() {
        wiremock.stubFor(post("/later").willReturn(aResponse().withStatus(503)));
        String workflowId = runs.publish(Map.of("tasks", List.of(call("call", "/later", 1),
            Runs.task("after", "DELAY", Map.of("durationMs", 0), null, "call"))));
        String id = runs.execute(workflowId, null);
        runs.awaitStatus(id, Duration.ofSeconds(15), "FAILED");
        String entryId = deadLetters(workflowId, false).get(0).get("id").asText();

        wiremock.stubFor(post("/later").willReturn(aResponse().withStatus(200)));
        ResponseEntity<String> response = replay(entryId);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(api().read(response).get("replayedTasks")).extracting(JsonNode::asText).containsExactly("call");
        JsonNode execution = runs.awaitStatus(id, Duration.ofSeconds(15), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(Runs.task(execution, "call").get("attempt").asInt()).isEqualTo(2);
        assertThat(Runs.task(execution, "after").get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(deadLetters(workflowId, false)).isEmpty();
        JsonNode entry = deadLetters(workflowId, true).get(0);
        assertThat(entry.get("replayedAt")).isNotNull();
        String operatorId = api().read(api().get("/api/v1/auth/me", operator)).get("id").asText();
        assertThat(entry.get("replayedBy").asText()).isEqualTo(operatorId);
        assertThat(runs.events(id).stream().map(e -> e.get("type").asText())).contains("WorkflowRetried");
    }

    @Test
    @DisplayName("T13.7 a second replay → 409 ALREADY_REPLAYED; replay while another task is dead → 409 "
        + "OTHER_TASKS_FAILED; execution retry replays both → COMPLETED")
    void replayConflictsAndExecutionRetry() {
        // Slow failures, so both are claimed (RUNNING) before the first failure makes the execution FAILED.
        wiremock.stubFor(post("/x").willReturn(aResponse().withStatus(503).withFixedDelay(300)));
        wiremock.stubFor(post("/y").willReturn(aResponse().withStatus(503).withFixedDelay(300)));
        String workflowId = runs.publish(Map.of("tasks", List.of(call("x", "/x", 1), call("y", "/y", 1))));
        String id = runs.execute(workflowId, null);
        runs.awaitStatus(id, Duration.ofSeconds(15), "FAILED");
        // The second failure arrives after the execution is already FAILED; it is still recorded and dead-lettered.
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10))
            .until(() -> deadLetters(workflowId, false).size() == 2);

        ResponseEntity<String> blocked = replay(deadLetters(workflowId, false).get(0).get("id").asText());
        assertThat(blocked.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api().read(blocked).get("error").asText()).isEqualTo("OTHER_TASKS_FAILED");

        wiremock.stubFor(post("/x").willReturn(aResponse().withStatus(200)));
        wiremock.stubFor(post("/y").willReturn(aResponse().withStatus(200)));
        ResponseEntity<String> retried = api().send(HttpMethod.POST, "/api/v1/executions/" + id + "/retry", null, token);
        assertThat(retried.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(api().read(retried).get("replayedTasks")).extracting(JsonNode::asText)
            .containsExactlyInAnyOrder("x", "y");
        assertThat(runs.awaitStatus(id, Duration.ofSeconds(15), "COMPLETED", "FAILED").get("status").asText())
            .isEqualTo("COMPLETED");

        String entryId = deadLetters(workflowId, true).get(0).get("id").asText();
        ResponseEntity<String> again = replay(entryId);
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api().read(again).get("error").asText()).isEqualTo("ALREADY_REPLAYED");
        ResponseEntity<String> retryCompleted = api().send(HttpMethod.POST, "/api/v1/executions/" + id + "/retry", null,
            token);
        assertThat(retryCompleted.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api().read(retryCompleted).get("error").asText()).isEqualTo("ILLEGAL_STATE_TRANSITION");
    }
}

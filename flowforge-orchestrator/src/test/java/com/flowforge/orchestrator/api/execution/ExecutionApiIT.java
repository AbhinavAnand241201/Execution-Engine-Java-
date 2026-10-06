package com.flowforge.orchestrator.api.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

class ExecutionApiIT extends OrchestratorIntegrationTestBase {

    private static String token;
    private ApiClient api;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void logIn() {
        api = api();
        token = token == null ? api.newUserToken() : token;
    }

    private String createWorkflow(Map<String, Object> definition, boolean publish) {
        String id = api.read(api.send(HttpMethod.POST, "/api/v1/workflows",
            Definitions.createRequest(Definitions.uniqueName("exec"), definition), token)).get("id").asText();
        if (publish) {
            assertThat(api.send(HttpMethod.POST, "/api/v1/workflows/" + id + "/publish", null, token).getStatusCode())
                .isEqualTo(HttpStatus.OK);
        }
        return id;
    }

    private ResponseEntity<String> execute(String workflowId, Object body, String idempotencyKey) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setBearerAuth(token);
        if (idempotencyKey != null) {
            headers.set("Idempotency-Key", idempotencyKey);
        }
        return rest.exchange("/api/v1/workflows/" + workflowId + "/execute", HttpMethod.POST,
            new HttpEntity<>(body, headers), String.class);
    }

    private Map<String, String> statusByKey(JsonNode execution) {
        Map<String, String> result = new HashMap<>();
        execution.get("tasks").forEach(t -> result.put(t.get("key").asText(), t.get("status").asText()));
        return result;
    }

    @Test
    @DisplayName("T09.1 executing a published diamond → 202 + Location, RUNNING, root READY and the rest PENDING")
    void executeCreatesRunningExecution() {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        ResponseEntity<String> response = execute(workflowId, Map.of("input", Map.of("orderId", 42)), null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        JsonNode body = api.read(response);
        assertThat(response.getHeaders().getLocation()).hasPath("/api/v1/executions/" + body.get("id").asText());
        assertThat(body.get("status").asText()).isEqualTo("RUNNING");
        assertThat(body.get("version").asInt()).isEqualTo(1);
        assertThat(body.get("input").get("orderId").asInt()).isEqualTo(42);
        assertThat(body.get("startedAt")).isNotNull();
        assertThat(statusByKey(body)).containsExactlyInAnyOrderEntriesOf(
            Map.of("a", "READY", "b", "PENDING", "c", "PENDING", "d", "PENDING", "e", "PENDING"));
        body.get("tasks").forEach(t -> {
            assertThat(t.get("maxAttempts").asInt()).isPositive();
            assertThat(t.get("attempt").asInt()).isZero();
        });

        JsonNode read = api.read(api.get(response.getHeaders().getLocation().getPath(), token));
        assertThat(read.get("id")).isEqualTo(body.get("id"));
    }

    @Test
    @DisplayName("T09.2 executing a workflow with only a draft → 409 WORKFLOW_NOT_PUBLISHED")
    void draftOnlyIsRejected() {
        String workflowId = createWorkflow(Definitions.diamond(), false);
        ResponseEntity<String> response = execute(workflowId, Map.of(), null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api.read(response).get("error").asText()).isEqualTo("WORKFLOW_NOT_PUBLISHED");
        assertThat(execute(workflowId, Map.of("version", 1), null).getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }

    @Test
    @DisplayName("T09.3 version=1 runs v1 while v2 is the latest published; no version runs v2")
    void pinnedVersion() {
        String workflowId = createWorkflow(Definitions.linear(2), true);
        long lock = api.read(api.get("/api/v1/workflows/" + workflowId + "/versions/1", token)).get("lockVersion").asLong();
        api.send(HttpMethod.PUT, "/api/v1/workflows/" + workflowId + "/draft",
            Map.of("definition", Definitions.linear(3), "lockVersion", lock), token);
        assertThat(api.send(HttpMethod.POST, "/api/v1/workflows/" + workflowId + "/publish", null, token)
            .getStatusCode()).isEqualTo(HttpStatus.OK);

        JsonNode pinned = api.read(execute(workflowId, Map.of("version", 1), null));
        assertThat(pinned.get("version").asInt()).isEqualTo(1);
        assertThat(pinned.get("tasks")).hasSize(2);

        JsonNode latest = api.read(execute(workflowId, Map.of(), null));
        assertThat(latest.get("version").asInt()).isEqualTo(2);
        assertThat(latest.get("tasks")).hasSize(3);
    }

    @Test
    @DisplayName("T09.4 the same Idempotency-Key twice → same execution id, one row, second response replayed")
    void idempotentReplay() {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        String key = "key-" + UUID.randomUUID();
        // Raw bodies whose key order differs, nested too: the hash is of the canonical form.
        ResponseEntity<String> first = execute(workflowId, "{\"input\":{\"a\":1,\"b\":{\"x\":1,\"y\":2}}}", key);
        ResponseEntity<String> second = execute(workflowId, "{\"input\":{\"b\":{\"y\":2,\"x\":1},\"a\":1}}", key);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(api.read(second).get("id")).isEqualTo(api.read(first).get("id"));
        assertThat(rows(key)).isEqualTo(1);
    }

    @Test
    @DisplayName("T09.5 the same Idempotency-Key with a different body → 422 IDEMPOTENCY_KEY_REUSED")
    void keyReusedWithDifferentBody() {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        String key = "key-" + UUID.randomUUID();
        execute(workflowId, Map.of("input", Map.of("a", 1)), key);

        ResponseEntity<String> reused = execute(workflowId, Map.of("input", Map.of("a", 2)), key);
        assertThat(reused.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(api.read(reused).get("error").asText()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(rows(key)).isEqualTo(1);
    }

    @Test
    @DisplayName("T09.6 20 concurrent requests with one Idempotency-Key (start gate, 50 iterations) → one row, one id")
    void concurrentIdempotency() throws Exception {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        List<String> keys = new ArrayList<>();
        List<List<Object>> outcomes = ConcurrencyHarness.race(20, 50, thread -> {
            ResponseEntity<String> response = execute(workflowId, Map.of("input", Map.of("x", 1)), keys.getLast());
            return response.getStatusCode().value() + " " + api.read(response).get("id").asText();
        }, () -> keys.add("key-" + UUID.randomUUID()));

        for (int i = 0; i < outcomes.size(); i++) {
            List<Object> iteration = outcomes.get(i);
            assertThat(iteration).allSatisfy(o -> assertThat(o).isInstanceOf(String.class));
            assertThat(iteration.stream().map(o -> ((String) o).split(" ")[1]).distinct()).as("iteration %d", i).hasSize(1);
            assertThat(iteration.stream().filter(o -> ((String) o).startsWith("202"))).as("iteration %d", i).hasSize(1);
            assertThat(rows(keys.get(i))).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("T09.7 a second task row with the same (execution, task_key) is rejected by the database")
    void uniqueTaskKeyPerExecution() {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        UUID executionId = UUID.fromString(api.read(execute(workflowId, Map.of(), null)).get("id").asText());

        assertThatThrownBy(() -> jdbc.update("""
            INSERT INTO task_executions (id, workflow_execution_id, task_definition_id, task_key, type, status, priority,
                                         max_attempts, retry_policy, timeout_ms, updated_at)
            SELECT gen_random_uuid(), workflow_execution_id, task_definition_id, task_key, type, status, priority,
                   max_attempts, retry_policy, timeout_ms, now()
            FROM task_executions WHERE workflow_execution_id = ? AND task_key = 'a'
            """, executionId)).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    @DisplayName("T09.8 events come back in order: WorkflowStarted, then one TaskReady per root")
    void eventsInOrder() {
        Map<String, Object> twoRoots = Map.of("tasks", List.of(Definitions.delay("x", 0), Definitions.delay("y", 0),
            Definitions.delay("z", 0, "x", "y")));
        String workflowId = createWorkflow(twoRoots, true);
        String executionId = api.read(execute(workflowId, Map.of(), null)).get("id").asText();

        JsonNode page = api.read(api.get("/api/v1/executions/" + executionId + "/events", token));
        JsonNode items = page.get("items");
        assertThat(items).hasSize(3);
        assertThat(items.get(0).get("type").asText()).isEqualTo("WorkflowStarted");
        List<String> readyKeys = new ArrayList<>();
        long previous = Long.MIN_VALUE;
        for (JsonNode event : items) {
            assertThat(event.get("sequence").asLong()).isGreaterThan(previous);
            previous = event.get("sequence").asLong();
            if (event.get("type").asText().equals("TaskReady")) {
                readyKeys.add(event.get("data").get("taskKey").asText());
            }
        }
        assertThat(readyKeys).containsExactlyInAnyOrder("x", "y");
    }

    @Test
    @DisplayName("T09.9 another USER gets 404 for someone else's execution; an OPERATOR can read it and list it")
    void visibility() {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        String executionId = api.read(execute(workflowId, Map.of(), null)).get("id").asText();
        String path = "/api/v1/executions/" + executionId;

        String stranger = api.newUserToken();
        assertThat(api.get(path, stranger).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.get(path + "/tasks/a/attempts", stranger).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.send(HttpMethod.POST, "/api/v1/workflows/" + workflowId + "/execute", Map.of(), stranger)
            .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.get(path + "/events", stranger).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.read(api.get("/api/v1/executions?workflowId=" + workflowId, stranger)).get("items")).isEmpty();
        assertThat(execute(workflowId, Map.of(), null).getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        String operator = api.operatorToken();
        assertThat(api.get(path, operator).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(api.read(api.get("/api/v1/executions?workflowId=" + workflowId, operator)).get("totalElements").asInt())
            .isEqualTo(2);
        assertThat(api.read(api.get("/api/v1/executions?workflowId=" + workflowId + "&status=RUNNING", token))
            .get("totalElements").asInt()).isEqualTo(2);
        assertThat(api.read(api.get(path + "/tasks/a/attempts", token))).isEmpty();
        assertThat(api.get(path + "/tasks/ghost/attempts", token).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("T09.10 input larger than 64 KB → 413 PAYLOAD_TOO_LARGE")
    void inputTooLarge() {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        ResponseEntity<String> response = execute(workflowId, Map.of("input", Map.of("blob", "x".repeat(70_000))), null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
        assertThat(api.read(response).get("error").asText()).isEqualTo("PAYLOAD_TOO_LARGE");
    }

    @Test
    @DisplayName("T09.extra-1 version 0 or negative → 400; a body over the 1 MiB request cap → 413 before parsing, "
        + "with or without Content-Length")
    void badVersionAndHugeBodies() throws Exception {
        String workflowId = createWorkflow(Definitions.diamond(), true);
        ResponseEntity<String> zero = execute(workflowId, Map.of("version", 0), null);
        assertThat(zero.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(api.read(zero).get("error").asText()).isEqualTo("VALIDATION_FAILED");

        String huge = "{\"input\":{\"blob\":\"" + "x".repeat(2_000_000) + "\"}}";
        ResponseEntity<String> declared = execute(workflowId, huge, null);
        assertThat(declared.getStatusCode()).isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
        assertThat(api.read(declared).get("error").asText()).isEqualTo("PAYLOAD_TOO_LARGE");

        HttpRequest chunked = HttpRequest.newBuilder(URI.create(rest.getRootUri() + "/api/v1/workflows/" + workflowId
                + "/execute"))
            .header("Content-Type", "application/json").header("Authorization", "Bearer " + token)
            .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(huge.getBytes())))
            .build();
        HttpResponse<String> response = HttpClient.newHttpClient().send(chunked, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(413);
        assertThat(json.readTree(response.body()).get("error").asText()).isEqualTo("PAYLOAD_TOO_LARGE");
    }

    private int rows(String idempotencyKey) {
        return jdbc.queryForObject("SELECT count(*) FROM workflow_executions WHERE idempotency_key = ?", Integer.class,
            idempotencyKey);
    }
}

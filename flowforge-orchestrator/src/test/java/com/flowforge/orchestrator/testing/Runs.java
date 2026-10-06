package com.flowforge.orchestrator.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/** Create, publish and run workflows through the API, and wait for executions to settle. */
public final class Runs {

    private final ApiClient api;
    private final String token;

    public Runs(ApiClient api, String token) {
        this.api = api;
        this.token = token;
    }

    public String publish(Map<String, Object> definition) {
        String id = api.read(api.send(HttpMethod.POST, "/api/v1/workflows",
            Definitions.createRequest(Definitions.uniqueName("run"), definition), token)).get("id").asText();
        assertThat(api.send(HttpMethod.POST, "/api/v1/workflows/" + id + "/publish", null, token).getStatusCode())
            .isEqualTo(HttpStatus.OK);
        return id;
    }

    public String execute(String workflowId, Object input) {
        Map<String, Object> body = new HashMap<>();
        if (input != null) {
            body.put("input", input);
        }
        ResponseEntity<String> response = api.send(HttpMethod.POST, "/api/v1/workflows/" + workflowId + "/execute", body,
            token);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return api.read(response).get("id").asText();
    }

    /** Publishes and starts one execution. */
    public String run(Map<String, Object> definition) {
        return execute(publish(definition), null);
    }

    public JsonNode execution(String executionId) {
        return api.read(api.get("/api/v1/executions/" + executionId, token));
    }

    public JsonNode awaitStatus(String executionId, Duration timeout, String... statuses) {
        List<String> wanted = Arrays.asList(statuses);
        await().atMost(timeout).pollInterval(Duration.ofMillis(20))
            .until(() -> wanted.contains(execution(executionId).get("status").asText()));
        return execution(executionId);
    }

    public List<JsonNode> events(String executionId) {
        JsonNode items = api.read(api.get("/api/v1/executions/" + executionId + "/events?size=100", token)).get("items");
        return java.util.stream.StreamSupport.stream(items.spliterator(), false).toList();
    }

    public static JsonNode task(JsonNode execution, String key) {
        for (JsonNode task : execution.get("tasks")) {
            if (task.get("key").asText().equals(key)) {
                return task;
            }
        }
        throw new AssertionError("no task " + key);
    }

    /** A task as a definition map, with optional priority and HTTP/other config. */
    public static Map<String, Object> task(String key, String type, Map<String, Object> config, String priority,
                                           String... dependsOn) {
        Map<String, Object> task = new HashMap<>();
        task.put("key", key);
        task.put("type", type);
        task.put("config", config);
        task.put("dependsOn", List.of(dependsOn));
        if (priority != null) {
            task.put("priority", priority);
        }
        return task;
    }
}

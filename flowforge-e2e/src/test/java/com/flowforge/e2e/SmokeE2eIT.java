package com.flowforge.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Black-box smoke test against a running stack ({@code -De2e.baseUrl}, default http://localhost:8080): only HTTP and
 * JSON, no FlowForge classes. Register → log in → create, publish and run a diamond → it completes.
 */
class SmokeE2eIT {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String BASE_URL = System.getProperty("e2e.baseUrl", "http://localhost:8080");
    private final HttpClient http = HttpClient.newHttpClient();

    private JsonNode call(String method, String path, Object body, String token, int expected) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(BASE_URL + path))
            .header("Content-Type", "application/json")
            .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(method + " " + path + ": " + response.body()).isEqualTo(expected);
        return response.body().isEmpty() ? null : JSON.readTree(response.body());
    }

    private static Map<String, Object> delay(String key, String... dependsOn) {
        return Map.of("key", key, "type", "DELAY", "config", Map.of("durationMs", 100), "dependsOn", List.of(dependsOn));
    }

    @Test
    @DisplayName("E2E smoke: register, log in, create + publish + execute a diamond → COMPLETED")
    void diamondCompletes() throws Exception {
        String email = "e2e-" + UUID.randomUUID() + "@example.com";
        String password = "e2e-password-" + UUID.randomUUID();
        call("POST", "/api/v1/auth/register", Map.of("email", email, "password", password), null, 201);
        String token = call("POST", "/api/v1/auth/login", Map.of("email", email, "password", password), null, 200)
            .get("accessToken").asText();

        Map<String, Object> definition = Map.of("tasks", List.of(delay("a"), delay("b", "a"), delay("c", "a"),
            delay("d", "a"), delay("e", "b", "c", "d")));
        String workflowId = call("POST", "/api/v1/workflows", Map.of("name", "e2e-" + UUID.randomUUID().toString()
            .substring(0, 8), "description", "e2e smoke", "definition", definition), token, 201).get("id").asText();
        call("POST", "/api/v1/workflows/" + workflowId + "/publish", null, token, 200);
        String executionId = call("POST", "/api/v1/workflows/" + workflowId + "/execute", Map.of(), token, 202)
            .get("id").asText();

        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(250)).until(() ->
            "COMPLETED".equals(call("GET", "/api/v1/executions/" + executionId, null, token, 200)
                .get("status").asText()));
        JsonNode execution = call("GET", "/api/v1/executions/" + executionId, null, token, 200);
        execution.get("tasks").forEach(t -> assertThat(t.get("status").asText()).isEqualTo("SUCCEEDED"));
    }
}

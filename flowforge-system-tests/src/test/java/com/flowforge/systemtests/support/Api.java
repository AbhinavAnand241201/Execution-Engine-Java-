package com.flowforge.systemtests.support;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** A black-box HTTP client of the orchestrator API: what a real user of the system does. */
public final class Api {

    private static final ObjectMapper JSON = new ObjectMapper();
    private final HttpClient http = HttpClient.newHttpClient();
    private final String baseUrl;
    private final String token;

    private Api(String baseUrl, String token) {
        this.baseUrl = baseUrl;
        this.token = token;
    }

    /** Registers a fresh user and logs in. */
    public static Api newUser(String baseUrl) {
        Api anonymous = new Api(baseUrl, null);
        String email = "sys-" + UUID.randomUUID() + "@example.com";
        String password = "system-test-password";
        anonymous.call("POST", "/api/v1/auth/register", Map.of("email", email, "password", password), 201);
        String token = anonymous.call("POST", "/api/v1/auth/login", Map.of("email", email, "password", password), 200)
            .get("accessToken").asText();
        return new Api(baseUrl, token);
    }

    /** An existing account (e.g. the seeded operator). */
    public static Api login(String baseUrl, String email, String password) {
        String token = new Api(baseUrl, null).call("POST", "/api/v1/auth/login", Map.of("email", email,
            "password", password), 200).get("accessToken").asText();
        return new Api(baseUrl, token);
    }

    /** Same user, another orchestrator instance (e.g. after a restart). */
    public Api at(String newBaseUrl) {
        return new Api(newBaseUrl, token);
    }

    public JsonNode call(String method, String path, Object body, int expectedStatus) {
        try {
            HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
            if (token != null) {
                request.header("Authorization", "Bearer " + token);
            }
            HttpResponse<String> response = http.send(request.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != expectedStatus) {
                throw new AssertionError(method + " " + path + " → " + response.statusCode() + ": " + response.body());
            }
            return response.body().isEmpty() ? null : JSON.readTree(response.body());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** Creates and publishes a workflow; returns its id. */
    public String publish(Map<String, Object> definition) {
        String id = call("POST", "/api/v1/workflows", Map.of("name", "sys-" + UUID.randomUUID().toString()
            .substring(0, 8), "description", "system test", "definition", definition), 201).get("id").asText();
        call("POST", "/api/v1/workflows/" + id + "/publish", null, 200);
        return id;
    }

    public String execute(String workflowId) {
        return call("POST", "/api/v1/workflows/" + workflowId + "/execute", Map.of(), 202).get("id").asText();
    }

    public String run(Map<String, Object> definition) {
        return execute(publish(definition));
    }

    public JsonNode execution(String id) {
        return call("GET", "/api/v1/executions/" + id, null, 200);
    }

    public JsonNode awaitStatus(String id, Duration timeout, String... statuses) {
        List<String> wanted = Arrays.asList(statuses);
        await().atMost(timeout).pollInterval(Duration.ofMillis(100))
            .until(() -> wanted.contains(execution(id).get("status").asText()));
        return execution(id);
    }

    public static JsonNode task(JsonNode execution, String key) {
        for (JsonNode task : execution.get("tasks")) {
            if (task.get("key").asText().equals(key)) {
                return task;
            }
        }
        throw new AssertionError("no task " + key);
    }

    public static Map<String, Object> delay(String key, long ms, String... dependsOn) {
        return Map.of("key", key, "type", "DELAY", "config", Map.of("durationMs", ms), "dependsOn", List.of(dependsOn));
    }
}

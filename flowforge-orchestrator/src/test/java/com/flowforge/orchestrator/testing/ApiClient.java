package com.flowforge.orchestrator.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

/**
 * Test helper for talking to the API as a real client would: register, log in, call endpoints with a
 * bearer token. Every test uses unique emails so tests stay independent of each other and of run order.
 */
public final class ApiClient {

    public static final String ADMIN_EMAIL = "admin@test.flowforge.local";
    public static final String ADMIN_PASSWORD = "test-admin-password";
    public static final String OPERATOR_EMAIL = "operator@test.flowforge.local";
    public static final String OPERATOR_PASSWORD = "test-operator-password";
    public static final String PASSWORD = "correct-horse-battery";

    private final TestRestTemplate rest;
    private final ObjectMapper json;

    public ApiClient(TestRestTemplate rest, ObjectMapper json) {
        this.rest = rest;
        this.json = json;
    }

    public static String uniqueEmail() {
        return "user-" + UUID.randomUUID() + "@example.com";
    }

    public ResponseEntity<String> register(String email, String password) {
        return send(HttpMethod.POST, "/api/v1/auth/register", Map.of("email", email, "password", password), null);
    }

    public ResponseEntity<String> login(String email, String password) {
        return send(HttpMethod.POST, "/api/v1/auth/login", Map.of("email", email, "password", password), null);
    }

    /** Registers a fresh USER and returns its access token. */
    public String newUserToken() {
        String email = uniqueEmail();
        register(email, PASSWORD);
        return token(email, PASSWORD);
    }

    public String token(String email, String password) {
        return read(login(email, password)).get("accessToken").asText();
    }

    public String adminToken() {
        return token(ADMIN_EMAIL, ADMIN_PASSWORD);
    }

    public String operatorToken() {
        return token(OPERATOR_EMAIL, OPERATOR_PASSWORD);
    }

    public ResponseEntity<String> get(String path, String token) {
        return send(HttpMethod.GET, path, null, token);
    }

    public ResponseEntity<String> send(HttpMethod method, String path, Object body, String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (token != null) {
            headers.setBearerAuth(token);
        }
        return rest.exchange(path, method, new HttpEntity<>(body, headers), String.class);
    }

    public JsonNode read(ResponseEntity<String> response) {
        try {
            return json.readTree(response.getBody());
        } catch (Exception e) {
            throw new AssertionError("not JSON: " + response.getBody(), e);
        }
    }
}

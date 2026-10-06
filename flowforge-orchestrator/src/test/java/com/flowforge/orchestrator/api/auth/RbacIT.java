package com.flowforge.orchestrator.api.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class RbacIT extends OrchestratorIntegrationTestBase {

    private static final String[] OPERATOR_PATHS = {"/api/v1/workers", "/api/v1/dead-letter/tasks"};

    @Test
    @DisplayName("T07.4 USER → 403 on /workers and /dead-letter/tasks; OPERATOR and ADMIN get past security "
        + "(never 401/403)")
    void operatorEndpoints() {
        ApiClient api = api();
        String user = api.newUserToken();
        String operator = api.operatorToken();
        String admin = api.adminToken();

        for (String path : OPERATOR_PATHS) {
            ResponseEntity<String> denied = api.get(path, user);
            assertThat(denied.getStatusCode()).as(path).isEqualTo(HttpStatus.FORBIDDEN);
            assertThat(api.read(denied).get("error").asText()).isEqualTo("FORBIDDEN");

            // Past security: whatever the endpoint answers (200 once it exists, 404 before), never 401/403.
            assertThat(api.get(path, operator).getStatusCode()).as(path)
                .isNotIn(HttpStatus.UNAUTHORIZED, HttpStatus.FORBIDDEN);
            assertThat(api.get(path, admin).getStatusCode()).as(path)
                .isNotIn(HttpStatus.UNAUTHORIZED, HttpStatus.FORBIDDEN);
        }
    }

    @Test
    @DisplayName("T07.4 ADMIN is allowed everywhere: role changes (ADMIN only), actuator metrics; OPERATOR and USER are not")
    void adminEndpoints() {
        ApiClient api = api();
        String userEmail = ApiClient.uniqueEmail();
        String userId = api.read(api.register(userEmail, ApiClient.PASSWORD)).get("id").asText();
        String path = "/api/v1/admin/users/" + userId + "/role";

        assertThat(api.send(HttpMethod.PATCH, path, Map.of("role", "OPERATOR"), api.token(userEmail, ApiClient.PASSWORD))
            .getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(api.send(HttpMethod.PATCH, path, Map.of("role", "OPERATOR"), api.operatorToken()).getStatusCode())
            .isEqualTo(HttpStatus.FORBIDDEN);

        ResponseEntity<String> changed = api.send(HttpMethod.PATCH, path, Map.of("role", "OPERATOR"), api.adminToken());
        assertThat(changed.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(api.read(changed).get("role").asText()).isEqualTo("OPERATOR");
        // Roles are carried in the token, so the new role applies from the next login.
        assertThat(api.get("/api/v1/workers", api.token(userEmail, ApiClient.PASSWORD)).getStatusCode())
            .isEqualTo(HttpStatus.OK);

        assertThat(api.get("/actuator/prometheus", api.adminToken()).getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(api.get("/actuator/prometheus", api.operatorToken()).getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(api.get("/actuator/prometheus", null).getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(api.get("/actuator/health/readiness", null).getStatusCode()).isEqualTo(HttpStatus.OK);

        // Health component details (e.g. the database product) are for ADMIN only.
        assertThat(api.read(api.get("/actuator/health", api.adminToken())).path("components").path("db").has("details"))
            .isTrue();
        assertThat(api.read(api.get("/actuator/health", null)).path("components").path("db").has("details")).isFalse();
    }

    @Test
    @DisplayName("T07.extra-1 role change for an unknown user → 404; invalid role → 400")
    void roleChangeErrors() {
        ApiClient api = api();
        String admin = api.adminToken();

        assertThat(api.send(HttpMethod.PATCH, "/api/v1/admin/users/00000000-0000-4000-8000-000000000000/role",
            Map.of("role", "OPERATOR"), admin).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        String userId = api.read(api.register(ApiClient.uniqueEmail(), ApiClient.PASSWORD)).get("id").asText();
        assertThat(api.send(HttpMethod.PATCH, "/api/v1/admin/users/" + userId + "/role", Map.of("role", "ROOT"), admin)
            .getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }
}

package com.flowforge.orchestrator.api.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class WorkflowApiIT extends OrchestratorIntegrationTestBase {

    private static String token;
    private ApiClient api;

    @BeforeEach
    void logIn() {
        api = api();
        token = token == null ? api.newUserToken() : token;
    }

    private ResponseEntity<String> create(Map<String, Object> body) {
        return api.send(HttpMethod.POST, "/api/v1/workflows", body, token);
    }

    @Test
    @DisplayName("T08.1 creating a valid workflow → 201 with Location, v1 DRAFT")
    void createValid() {
        String name = Definitions.uniqueName("valid");
        ResponseEntity<String> response = create(Definitions.createRequest(name, Definitions.diamond()));

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode body = api.read(response);
        assertThat(response.getHeaders().getLocation()).hasPath("/api/v1/workflows/" + body.get("id").asText());
        assertThat(body.get("name").asText()).isEqualTo(name);
        assertThat(body.get("latestVersion").asInt()).isEqualTo(1);
        assertThat(body.has("latestPublishedVersion")).isFalse();
        JsonNode v1 = body.get("versions").get(0);
        assertThat(v1.get("version").asInt()).isEqualTo(1);
        assertThat(v1.get("status").asText()).isEqualTo("DRAFT");
        assertThat(v1.get("lockVersion").asLong()).isZero();

        ResponseEntity<String> duplicate = create(Definitions.createRequest(name, Definitions.diamond()));
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api.read(duplicate).get("error").asText()).isEqualTo("WORKFLOW_NAME_TAKEN");
    }

    @Test
    @DisplayName("T08.2 cycle → 422 CYCLE_DETECTED with the path; unknown dependency → 422 with its JSON path")
    void invalidDefinitions() {
        Map<String, Object> cyclic = Map.of("tasks", List.of(
            Definitions.delay("a", 0, "c"), Definitions.delay("b", 0, "a"), Definitions.delay("c", 0, "b")));
        ResponseEntity<String> cycle = create(Definitions.createRequest(Definitions.uniqueName("cycle"), cyclic));

        assertThat(cycle.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        JsonNode cycleBody = api.read(cycle);
        assertThat(cycleBody.get("error").asText()).isEqualTo("INVALID_WORKFLOW_DEFINITION");
        JsonNode detail = cycleBody.get("details").get(0);
        assertThat(detail.get("code").asText()).isEqualTo("CYCLE_DETECTED");
        assertThat(detail.get("message").asText()).contains("a → b → c → a");

        Map<String, Object> unknown = Map.of("tasks", List.of(Definitions.delay("a", 0), Definitions.delay("b", 0, "ghost")));
        JsonNode unknownBody = api.read(create(Definitions.createRequest(Definitions.uniqueName("unknown"), unknown)));
        assertThat(unknownBody.get("details").get(0).get("field").asText()).isEqualTo("tasks[1].dependsOn[0]");
        assertThat(unknownBody.get("details").get(0).get("code").asText()).isEqualTo("UNKNOWN_DEPENDENCY");
    }

    @Test
    @DisplayName("T08.extra-1 bad workflow names → 400; unknown field inside the definition → 422 with its path")
    void badNamesAndUnknownFields() {
        assertThat(create(Definitions.createRequest("Bad Name", Definitions.diamond())).getStatusCode())
            .isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(create(Definitions.createRequest("ab", Definitions.diamond())).getStatusCode())
            .isEqualTo(HttpStatus.BAD_REQUEST);

        Map<String, Object> withTypo = Map.of("tasks", List.of(Map.of("key", "a", "type", "DELAY",
            "config", Map.of("durationMs", 1), "timeoutMS", 500)));
        JsonNode body = api.read(create(Definitions.createRequest(Definitions.uniqueName("typo"), withTypo)));
        assertThat(body.get("error").asText()).isEqualTo("INVALID_WORKFLOW_DEFINITION");
        assertThat(body.get("details").get(0).get("field").asText()).isEqualTo("tasks[0].timeoutMS");
    }

    @Test
    @DisplayName("T08.10 the validate endpoint on a diamond → valid, levels [[a],[b,c,d],[e]]; nothing is stored")
    void validateDiamond() {
        JsonNode report = api.read(api.send(HttpMethod.POST, "/api/v1/workflows/validate",
            Map.of("definition", Definitions.diamond()), token));

        assertThat(report.get("valid").asBoolean()).isTrue();
        assertThat(report.get("errors")).isEmpty();
        assertThat(json.convertValue(report.get("levels"), List.class))
            .isEqualTo(List.of(List.of("a"), List.of("b", "c", "d"), List.of("e")));

        JsonNode invalid = api.read(api.send(HttpMethod.POST, "/api/v1/workflows/validate",
            Map.of("definition", Map.of("tasks", List.of(Definitions.delay("a", 0, "a")))), token));
        assertThat(invalid.get("valid").asBoolean()).isFalse();
        assertThat(invalid.get("errors").get(0).get("code").asText()).isEqualTo("SELF_DEPENDENCY");
        assertThat(invalid.get("levels")).isEmpty();
    }

    @Test
    @DisplayName("T08.11 a DELAY task with negative durationMs → 422 INVALID_TASK_CONFIG (executor validation wired in)")
    void executorConfigValidation() {
        Map<String, Object> negative = Map.of("tasks", List.of(Definitions.delay("a", -5)));
        JsonNode body = api.read(create(Definitions.createRequest(Definitions.uniqueName("neg"), negative)));

        assertThat(body.get("status").asInt()).isEqualTo(422);
        assertThat(body.get("details").get(0).get("code").asText()).isEqualTo("INVALID_TASK_CONFIG");
        assertThat(body.get("details").get(0).get("field").asText()).isEqualTo("tasks[0].config.durationMs");
    }

    @Test
    @DisplayName("T08.extra-2 another user's workflow is 404 for get, version, draft and publish")
    void ownership() {
        String id = api.read(create(Definitions.createRequest(Definitions.uniqueName("mine"), Definitions.diamond())))
            .get("id").asText();
        String other = api.newUserToken();

        assertThat(api.get("/api/v1/workflows/" + id, other).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.get("/api/v1/workflows/" + id + "/versions/1", other).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.send(HttpMethod.PUT, "/api/v1/workflows/" + id + "/draft",
            Map.of("definition", Definitions.diamond(), "lockVersion", 0), other).getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.send(HttpMethod.POST, "/api/v1/workflows/" + id + "/publish", null, other).getStatusCode())
            .isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.get("/api/v1/workflows/" + id, token).getStatusCode()).isEqualTo(HttpStatus.OK);
    }
}

package com.flowforge.orchestrator.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

@ExtendWith(OutputCaptureExtension.class)
class ApiFoundationIT extends OrchestratorIntegrationTestBase {

    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    /** Everything but auth, docs and health is secured; these tests call the API as a logged-in user. */
    private static String token;

    @BeforeEach
    void logIn() {
        token = token == null ? api().newUserToken() : token;
    }

    @Test
    @DisplayName("T06.1 unknown route → 404 in the standard error format with a correlationId")
    void unknownRoute() throws Exception {
        ResponseEntity<String> response = get("/api/v1/does-not-exist", null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        JsonNode body = json.readTree(response.getBody());
        assertThat(body.get("status").asInt()).isEqualTo(404);
        assertThat(body.get("error").asText()).isEqualTo("NOT_FOUND");
        assertThat(body.get("path").asText()).isEqualTo("/api/v1/does-not-exist");
        assertThat(body.get("timestamp").asText()).isNotBlank();
        assertThat(body.get("correlationId").asText()).isEqualTo(response.getHeaders().getFirst("X-Correlation-Id"));
    }

    @Test
    @DisplayName("T06.2 malformed JSON → 400 MALFORMED_REQUEST")
    void malformedJson() throws Exception {
        ResponseEntity<String> response = post("/api/v1/test/echo", "{\"name\": ", null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(json.readTree(response.getBody()).get("error").asText()).isEqualTo("MALFORMED_REQUEST");
    }

    @Test
    @DisplayName("T06.extra-1 bean validation failure → 400 VALIDATION_FAILED with field details; unknown field → 400")
    void validationFailure() throws Exception {
        JsonNode body = json.readTree(post("/api/v1/test/echo", "{\"name\": \"\"}", null).getBody());
        assertThat(body.get("error").asText()).isEqualTo("VALIDATION_FAILED");
        assertThat(body.get("details").get(0).get("field").asText()).isEqualTo("name");

        ResponseEntity<String> unknown = post("/api/v1/test/echo", "{\"name\": \"x\", \"extra\": 1}", null);
        assertThat(unknown.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(json.readTree(unknown.getBody()).get("error").asText()).isEqualTo("MALFORMED_REQUEST");
    }

    @Test
    @DisplayName("T06.3 correlation id: provided → echoed; missing → generated UUID; invalid → replaced; in the request's logs")
    void correlationId(CapturedOutput output) {
        String provided = "req-" + UUID.randomUUID();
        ResponseEntity<String> echoed = get("/api/v1/test/log", provided);
        assertThat(echoed.getHeaders().getFirst("X-Correlation-Id")).isEqualTo(provided);
        assertThat(output.getOut()).containsPattern("\\[" + provided + "\\].*test endpoint hit");

        String generated = get("/api/v1/test/log", null).getHeaders().getFirst("X-Correlation-Id");
        assertThat(generated).matches(UUID_PATTERN);

        String replaced = get("/api/v1/test/log", "not valid!").getHeaders().getFirst("X-Correlation-Id");
        assertThat(replaced).matches(UUID_PATTERN);
    }

    @Test
    @DisplayName("T06.4 a controller throwing RuntimeException(\"secret detail\") → 500 INTERNAL_ERROR without internals")
    void internalErrorHidesDetails() throws Exception {
        ResponseEntity<String> response = get("/api/v1/test/boom", null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(json.readTree(response.getBody()).get("error").asText()).isEqualTo("INTERNAL_ERROR");
        assertThat(response.getBody()).doesNotContain("secret detail", "IllegalStateException", "at com.", "trace");
    }

    @Test
    @DisplayName("T06.5 readiness includes the database; liveness does not")
    void healthGroups() throws Exception {
        JsonNode readiness = json.readTree(rest.getForEntity("/actuator/health/readiness", String.class).getBody());
        JsonNode liveness = json.readTree(rest.getForEntity("/actuator/health/liveness", String.class).getBody());

        assertThat(readiness.get("status").asText()).isEqualTo("UP");
        assertThat(readiness.get("components").has("db")).isTrue();
        assertThat(liveness.get("status").asText()).isEqualTo("UP");
        assertThat(liveness.path("components").has("db")).isFalse();
    }

    @Test
    @DisplayName("T06.6 /v3/api-docs is served and declares the bearer security scheme")
    void openApiDocs() throws Exception {
        JsonNode docs = json.readTree(rest.getForEntity("/v3/api-docs", String.class).getBody());

        JsonNode scheme = docs.get("components").get("securitySchemes").get("bearerAuth");
        assertThat(scheme.get("type").asText()).isEqualTo("http");
        assertThat(scheme.get("scheme").asText()).isEqualTo("bearer");
        assertThat(scheme.get("bearerFormat").asText()).isEqualTo("JWT");
    }

    @Test
    @DisplayName("T06.7 size=101 → 400; unknown sort field → 400 INVALID_SORT; valid paging → standard page shape")
    void pagingRules() throws Exception {
        ResponseEntity<String> tooBig = get("/api/v1/test/things?size=101", null);
        assertThat(tooBig.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(json.readTree(tooBig.getBody()).get("error").asText()).isEqualTo("VALIDATION_FAILED");

        ResponseEntity<String> badSort = get("/api/v1/test/things?sort=password,asc", null);
        assertThat(badSort.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(json.readTree(badSort.getBody()).get("error").asText()).isEqualTo("INVALID_SORT");

        JsonNode page = json.readTree(get("/api/v1/test/things?page=0&size=5&sort=name,desc", null).getBody());
        assertThat(page.get("items").isArray()).isTrue();
        assertThat(page.get("page").asInt()).isZero();
        assertThat(page.get("size").asInt()).isEqualTo(5);
        assertThat(page.get("totalElements").asLong()).isZero();
        assertThat(page.get("totalPages").asInt()).isZero();
    }

    @Test
    @DisplayName("T06.extra-3 405 (standard body + Allow), 415 (standard body), 406 (status only: the client refuses JSON)")
    void protocolErrors() throws Exception {
        ResponseEntity<String> wrongMethod = rest.exchange("/api/v1/system/info", HttpMethod.DELETE, null, String.class);
        assertThat(wrongMethod.getStatusCode()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
        assertThat(json.readTree(wrongMethod.getBody()).get("error").asText()).isEqualTo("METHOD_NOT_ALLOWED");
        assertThat(wrongMethod.getHeaders().getAllow()).contains(HttpMethod.GET);

        HttpHeaders textPlain = new HttpHeaders();
        textPlain.setContentType(MediaType.TEXT_PLAIN);
        textPlain.setBearerAuth(token);
        ResponseEntity<String> wrongType = rest.exchange("/api/v1/test/echo", HttpMethod.POST,
            new HttpEntity<>("name=x", textPlain), String.class);
        assertThat(wrongType.getStatusCode()).isEqualTo(HttpStatus.UNSUPPORTED_MEDIA_TYPE);
        assertThat(json.readTree(wrongType.getBody()).get("error").asText()).isEqualTo("UNSUPPORTED_MEDIA_TYPE");

        HttpHeaders wantsXml = new HttpHeaders();
        wantsXml.setAccept(List.of(MediaType.APPLICATION_XML));
        wantsXml.setBearerAuth(token);
        ResponseEntity<String> notAcceptable = rest.exchange("/api/v1/test/status", HttpMethod.GET, new HttpEntity<>(wantsXml),
            String.class);
        assertThat(notAcceptable.getStatusCode()).isEqualTo(HttpStatus.NOT_ACCEPTABLE);
    }

    @Test
    @DisplayName("T06.extra-4 Spring's own status exceptions keep their status and the standard body (not a 500)")
    void springStatusExceptions() throws Exception {
        ResponseEntity<String> tooLarge = get("/api/v1/test/status", null);

        assertThat(tooLarge.getStatusCode()).isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
        JsonNode body = json.readTree(tooLarge.getBody());
        assertThat(body.get("error").asText()).isEqualTo("PAYLOAD_TOO_LARGE");
        assertThat(body.get("correlationId").asText()).isNotBlank();
    }

    @Test
    @DisplayName("T06.extra-5 an exception thrown by a servlet filter (outside Spring MVC) still gets the standard 500 body "
        + "with the correlation id")
    void filterExceptionUsesStandardBody(CapturedOutput output) throws Exception {
        ResponseEntity<String> response = get("/api/v1/test/filter-boom", "filter-boom-1");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        JsonNode body = json.readTree(response.getBody());
        assertThat(body.get("error").asText()).isEqualTo("INTERNAL_ERROR");
        assertThat(body.get("correlationId").asText()).isEqualTo("filter-boom-1");
        assertThat(body.get("path").asText()).isEqualTo("/api/v1/test/filter-boom");
        assertThat(response.getBody()).doesNotContain("filter secret");
        assertThat(output.getOut()).containsPattern("\\[filter-boom-1\\].*unhandled error path=/api/v1/test/filter-boom");
    }

    @Test
    @DisplayName("T06.extra-2 GET /api/v1/system/info is public and reports name and version")
    void systemInfo() throws Exception {
        JsonNode info = json.readTree(rest.getForEntity("/api/v1/system/info", String.class).getBody());

        assertThat(info.get("name").asText()).isEqualTo("flowforge-orchestrator");
        assertThat(info.get("version").asText()).isNotBlank();
        assertThat(info.has("commit")).isTrue();
    }

    private ResponseEntity<String> get(String path, String correlationId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        if (correlationId != null) {
            headers.set("X-Correlation-Id", correlationId);
        }
        return rest.exchange(path, HttpMethod.GET, new HttpEntity<>(headers), String.class);
    }

    private ResponseEntity<String> post(String path, String body, String correlationId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(token);
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (correlationId != null) {
            headers.set("X-Correlation-Id", correlationId);
        }
        return rest.exchange(path, HttpMethod.POST, new HttpEntity<>(body, headers), String.class);
    }
}

package com.flowforge.orchestrator.api.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.executor.CancellationToken;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.runtime.executor.DatabaseTaskExecutor;
import com.flowforge.runtime.executor.ReportTaskExecutor;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class ExecutorWiringIT extends OrchestratorIntegrationTestBase {

    @Autowired
    private DatabaseTaskExecutor database;

    @Autowired
    private ReportTaskExecutor report;

    private ResponseEntity<String> create(ApiClient api, String token, List<Map<String, Object>> tasks) {
        return api.send(HttpMethod.POST, "/api/v1/workflows",
            Definitions.createRequest(Definitions.uniqueName("exec-types"), Map.of("tasks", tasks)), token);
    }

    @Test
    @DisplayName("T10.extra-3 the API validates every task type: allowlisted HTTP + DATABASE + REPORT → 201; "
        + "a non-allowlisted host → 422 at tasks[0].config.url; an unknown query → 422")
    void everyTypeIsValidated() {
        ApiClient api = api();
        String token = api.newUserToken();

        ResponseEntity<String> ok = create(api, token, List.of(
            Map.of("key", "fetch", "type", "HTTP", "config", Map.of("method", "GET", "url", "http://wiremock:8080/x")),
            Map.of("key", "orders", "type", "DATABASE", "config",
                Map.of("query", "orders_by_status", "params", Map.of("status", "PAID"))),
            Map.of("key", "summary", "type", "REPORT", "dependsOn", List.of("orders"), "config",
                Map.of("source", "orders", "groupBy", "customer", "metrics", List.of(Map.of("field", "amount", "fn", "sum"))))));
        assertThat(ok.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        JsonNode ssrf = api.read(create(api, token, List.of(Map.of("key", "a", "type", "HTTP", "config",
            Map.of("method", "GET", "url", "http://169.254.169.254/latest/meta-data")))));
        assertThat(ssrf.get("status").asInt()).isEqualTo(422);
        assertThat(ssrf.get("details").get(0).get("field").asText()).isEqualTo("tasks[0].config.url");

        JsonNode unknownQuery = api.read(create(api, token, List.of(Map.of("key", "a", "type", "DATABASE", "config",
            Map.of("query", "DROP TABLE users")))));
        assertThat(unknownQuery.get("details").get(0).get("field").asText()).isEqualTo("tasks[0].config.query");
    }

    @Test
    @DisplayName("T10.extra-4 data passing: the DATABASE executor reads V100's demo.orders and REPORT aggregates its rows")
    void databaseToReport() throws Exception {
        TaskResult rows = database.execute(context(json.readTree(
            "{\"query\":\"orders_by_status\",\"params\":{\"status\":\"PAID\"}}"), Map.of()));
        assertThat(rows).isInstanceOf(TaskResult.Success.class);
        JsonNode output = ((TaskResult.Success) rows).output();
        assertThat(output.get("rowCount").asInt()).isEqualTo(125);

        TaskResult summary = report.execute(context(json.readTree(
            "{\"source\":\"orders\",\"metrics\":[{\"field\":\"amount\",\"fn\":\"count\"}]}"), Map.of("orders", output)));
        assertThat(summary).isInstanceOfSatisfying(TaskResult.Success.class,
            s -> assertThat(s.output().get("totals").get("count_amount").asInt()).isEqualTo(125));
    }

    private static TaskContext context(JsonNode config, Map<String, JsonNode> upstream) {
        return new TaskContext(UUID.randomUUID(), UUID.randomUUID(), "t", 1, config, upstream, null,
            new CancellationToken(), null);
    }
}

package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.Contexts.json;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.executor.CancellationToken;
import com.flowforge.core.executor.TaskResult;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReportTaskExecutorTest {

    private final ReportTaskExecutor executor = new ReportTaskExecutor(Contexts.JSON);

    private static final JsonNode ROWS = json("""
        {"rows": [
          {"status": "PAID", "amount": 10.10},
          {"status": "NEW",  "amount": 5},
          {"status": "PAID", "amount": "4.90"},
          {"status": "NEW",  "amount": null}
        ], "rowCount": 4}
        """);

    private TaskResult run(String config, Map<String, JsonNode> upstream) {
        return executor.execute(Contexts.context(UUID.randomUUID(), 1, json(config), upstream, null,
            new CancellationToken()));
    }

    @Test
    @DisplayName("T10.8 count/sum/avg/min/max with groupBy and totals are exact on a fixture")
    void aggregates() {
        TaskResult result = run("""
            {"source": "orders", "groupBy": "status", "metrics": [
              {"field": "amount", "fn": "count"}, {"field": "amount", "fn": "sum"}, {"field": "amount", "fn": "avg"},
              {"field": "amount", "fn": "min"}, {"field": "amount", "fn": "max"}]}
            """, Map.of("orders", ROWS));

        assertThat(result).isInstanceOfSatisfying(TaskResult.Success.class, s -> {
            JsonNode paid = s.output().get("groups").get(0);
            assertThat(paid.get("status").asText()).isEqualTo("PAID");
            assertThat(paid.get("count_amount").asInt()).isEqualTo(2);
            assertThat(paid.get("sum_amount").decimalValue()).isEqualByComparingTo("15.00");
            assertThat(paid.get("avg_amount").decimalValue()).isEqualByComparingTo("7.5");
            assertThat(paid.get("min_amount").decimalValue()).isEqualByComparingTo("4.90");
            assertThat(paid.get("max_amount").decimalValue()).isEqualByComparingTo("10.10");

            JsonNode fresh = s.output().get("groups").get(1);
            assertThat(fresh.get("count_amount").asInt()).isEqualTo(1);
            assertThat(fresh.get("sum_amount").decimalValue()).isEqualByComparingTo("5");

            JsonNode totals = s.output().get("totals");
            assertThat(totals.get("count_amount").asInt()).isEqualTo(3);
            assertThat(totals.get("sum_amount").decimalValue()).isEqualByComparingTo("20.00");
            assertThat(totals.get("avg_amount").decimalValue()).isEqualByComparingTo("6.6667");
        });
    }

    @Test
    @DisplayName("T10.8 without groupBy only totals are produced; empty rows give count 0 and null min/avg")
    void totalsOnly() {
        TaskResult result = run("{\"source\":\"o\",\"metrics\":[{\"field\":\"amount\",\"fn\":\"count\"},"
            + "{\"field\":\"amount\",\"fn\":\"min\"},{\"field\":\"amount\",\"fn\":\"avg\"}]}",
            Map.of("o", json("{\"rows\":[]}")));
        assertThat(result).isInstanceOfSatisfying(TaskResult.Success.class, s -> {
            assertThat(s.output().get("groups")).isEmpty();
            assertThat(s.output().get("totals").get("count_amount").asInt()).isZero();
            assertThat(s.output().get("totals").get("min_amount").isNull()).isTrue();
            assertThat(s.output().get("totals").get("avg_amount").isNull()).isTrue();
        });
    }

    @Test
    @DisplayName("T10.8 a missing source (or one without rows) → non-retryable failure")
    void missingSource() {
        String config = "{\"source\":\"orders\",\"metrics\":[{\"field\":\"amount\",\"fn\":\"sum\"}]}";
        assertThat(run(config, Map.of())).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.errorCode()).isEqualTo("MISSING_SOURCE");
            assertThat(f.retryable()).isFalse();
        });
        assertThat(run(config, Map.of("orders", json("{\"status\":200}")))).isInstanceOf(TaskResult.Failure.class);
    }
}

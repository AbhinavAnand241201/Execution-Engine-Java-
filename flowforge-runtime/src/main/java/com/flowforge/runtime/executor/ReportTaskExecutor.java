package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.ConfigChecks.invalid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Aggregates the {@code rows} produced by an upstream task (usually DATABASE): optional {@code groupBy} plus
 * count/sum/avg/min/max metrics. This is how data flows between tasks: the runner hands every task its dependencies'
 * outputs by key, and REPORT reads {@code upstreamOutputs[source].rows}. Sums use {@link BigDecimal} so money adds up
 * exactly; averages are rounded to 4 decimal places.
 */
public final class ReportTaskExecutor implements TaskExecutor {

    enum Fn { count, sum, avg, min, max }

    private static final Set<String> FIELDS = Set.of("source", "groupBy", "metrics");
    private static final Set<String> METRIC_FIELDS = Set.of("field", "fn");
    private static final int MAX_SCALE = 1_000;

    private final ObjectMapper json;

    public ReportTaskExecutor(ObjectMapper json) {
        this.json = json;
    }

    @Override
    public TaskType supportedType() {
        return TaskType.REPORT;
    }

    @Override
    public List<ValidationError> validateConfig(JsonNode config) {
        List<ValidationError> errors = new ArrayList<>();
        ConfigChecks.requiredText(config, "source", errors);
        JsonNode groupBy = config.get("groupBy");
        if (groupBy != null && (!groupBy.isTextual() || groupBy.asText().isBlank())) {
            errors.add(invalid("groupBy", "groupBy must be a column name"));
        }
        JsonNode metrics = config.get("metrics");
        if (metrics == null || !metrics.isArray() || metrics.isEmpty()) {
            errors.add(invalid("metrics", "metrics is required and must be a non-empty array"));
        } else {
            for (int i = 0; i < metrics.size(); i++) {
                JsonNode metric = metrics.get(i);
                String path = "metrics[" + i + "]";
                if (!metric.isObject()) {
                    errors.add(invalid(path, "each metric must be an object {field, fn}"));
                    continue;
                }
                if (!metric.path("field").isTextual() || metric.path("field").asText().isBlank()) {
                    errors.add(invalid(path + ".field", "field is required"));
                }
                if (!metric.path("fn").isTextual() || fn(metric.path("fn").asText()) == null) {
                    errors.add(invalid(path + ".fn", "fn must be one of count, sum, avg, min, max"));
                }
                metric.fieldNames().forEachRemaining(f -> {
                    if (!METRIC_FIELDS.contains(f)) {
                        errors.add(invalid(path + "." + f, "unknown field '" + f + "'"));
                    }
                });
            }
        }
        ConfigChecks.rejectUnknownFields(config, FIELDS, errors);
        return errors;
    }

    private static Fn fn(String name) {
        for (Fn fn : Fn.values()) {
            if (fn.name().equals(name)) {
                return fn;
            }
        }
        return null;
    }

    @Override
    public TaskResult execute(TaskContext context) {
        JsonNode config = context.config();
        List<ValidationError> problems = validateConfig(config);
        if (!problems.isEmpty()) {
            return TaskResult.failure("INVALID_TASK_CONFIG", problems.getFirst().message(), false);
        }
        String source = config.get("source").asText();
        JsonNode upstream = context.upstreamOutputs().get(source);
        if (upstream == null || !upstream.path("rows").isArray()) {
            return TaskResult.failure("MISSING_SOURCE", "no rows from upstream task '" + source + "'", false);
        }
        String groupBy = config.path("groupBy").isTextual() ? config.get("groupBy").asText() : null;
        JsonNode metrics = config.get("metrics");

        Map<JsonNode, List<JsonNode>> groups = new LinkedHashMap<>();
        List<JsonNode> all = new ArrayList<>();
        for (JsonNode row : upstream.get("rows")) {
            all.add(row);
            if (groupBy != null) {
                // Keyed by the JSON value itself: 1 and "1" stay apart, and a missing value groups as a real null.
                JsonNode key = row.hasNonNull(groupBy) ? row.get(groupBy) : json.nullNode();
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
            }
        }
        ObjectNode output = json.createObjectNode();
        ArrayNode groupArray = output.putArray("groups");
        groups.forEach((key, rows) -> {
            ObjectNode group = groupArray.addObject();
            group.set(groupBy, key);
            aggregate(rows, metrics, group);
        });
        aggregate(all, metrics, output.putObject("totals"));
        return TaskResult.success(output);
    }

    /** Writes one value per metric, named {@code <fn>_<field>} (e.g. {@code sum_amount}). */
    private static void aggregate(List<JsonNode> rows, JsonNode metrics, ObjectNode target) {
        for (JsonNode metric : metrics) {
            String field = metric.get("field").asText();
            Fn fn = fn(metric.get("fn").asText());
            String name = fn + "_" + field;
            List<BigDecimal> values = new ArrayList<>();
            for (JsonNode row : rows) {
                JsonNode value = row.get(field);
                if (value != null && value.isNumber()) {
                    values.add(value.decimalValue());
                } else if (value != null && value.isTextual()) {
                    try {
                        BigDecimal parsed = new BigDecimal(value.asText());
                        // Reject absurd exponents ("1e999999999"), which would make add() run for ages.
                        if (Math.abs(parsed.scale()) <= MAX_SCALE) {
                            values.add(parsed);
                        }
                    } catch (NumberFormatException notANumber) {
                        // Non-numeric values only count towards "count".
                    }
                }
            }
            switch (fn) {
                case count -> target.put(name, rows.stream().filter(r -> r.hasNonNull(field)).count());
                case sum -> target.put(name, values.stream().reduce(BigDecimal.ZERO, BigDecimal::add));
                case avg -> {
                    if (values.isEmpty()) {
                        target.putNull(name);
                    } else {
                        target.put(name, values.stream().reduce(BigDecimal.ZERO, BigDecimal::add)
                            .divide(BigDecimal.valueOf(values.size()), 4, RoundingMode.HALF_UP));
                    }
                }
                case min -> putOrNull(target, name, values.stream().min(BigDecimal::compareTo).orElse(null));
                case max -> putOrNull(target, name, values.stream().max(BigDecimal::compareTo).orElse(null));
            }
        }
    }

    private static void putOrNull(ObjectNode target, String name, BigDecimal value) {
        if (value == null) {
            target.putNull(name);
        } else {
            target.put(name, value);
        }
    }
}

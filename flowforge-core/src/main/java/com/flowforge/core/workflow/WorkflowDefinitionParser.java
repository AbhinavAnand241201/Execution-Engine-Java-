package com.flowforge.core.workflow;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.CoercionAction;
import com.fasterxml.jackson.databind.cfg.CoercionInputShape;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.type.LogicalType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts the JSON workflow format to and from {@link WorkflowDefinition}. The JSON shape
 * ({@code timeoutMs}, {@code retry.initialDelayMs}) differs from the domain ({@link Duration}s), so private DTO
 * records sit in between and the domain records stay free of JSON annotations.
 *
 * <p>Parsing is strict and every failure carries a JSON path: unknown fields, duplicate fields, values of the wrong
 * JSON type (no string-to-number, number-to-enum or float-to-int coercion), null list elements and trailing content
 * are rejected (Jackson still reads the strings {@code "NaN"}/{@code "Infinity"} as doubles; the validator's
 * non-finite check rejects them). A lenient parser would silently turn a typo into a default, or {@code "type": 0} into HTTP.
 * Parsing runs in two phases, text to tree (syntax: malformed JSON, duplicate fields) and tree to records (binding:
 * unknown fields, wrong types), so each failure is classified by where it happened rather than by exception
 * message. Parse errors stop at the first problem; semantic rules are then checked collect-all by
 * {@link WorkflowDefinitionValidator}.
 */
public class WorkflowDefinitionParser {

    private final ObjectMapper mapper = strictMapper();

    /** Parses JSON text; throws {@link InvalidWorkflowDefinitionException} with a located error on bad input. */
    public WorkflowDefinition parse(String json) {
        if (json == null) {
            throw malformed();
        }
        JsonNode tree;
        try {
            tree = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new InvalidWorkflowDefinitionException(List.of(syntaxError(e)));
        }
        return parse(tree);
    }

    /** Parses an already-read JSON tree, e.g. a field of a larger request body. */
    public WorkflowDefinition parse(JsonNode json) {
        if (json == null || json.isNull() || json.isMissingNode()) {
            throw malformed();
        }
        try {
            return toDomain(mapper.treeToValue(json, DefinitionJson.class));
        } catch (JsonProcessingException e) {
            throw new InvalidWorkflowDefinitionException(List.of(bindingError(e)));
        }
    }

    /** Serializes with every default written out, so the stored JSON says exactly what will run. */
    public String toJson(WorkflowDefinition definition) {
        try {
            return mapper.writeValueAsString(toJsonTree(definition));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("definition records always serialize", e);
        }
    }

    public JsonNode toJsonTree(WorkflowDefinition definition) {
        List<TaskJson> tasks = definition.tasks().stream().map(WorkflowDefinitionParser::toDto).toList();
        return mapper.valueToTree(new DefinitionJson(tasks));
    }

    private static ObjectMapper strictMapper() {
        JsonMapper mapper = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .build();
        // Numbers and booleans must not silently become strings (e.g. "key": 123).
        mapper.coercionConfigFor(LogicalType.Textual)
            .setCoercion(CoercionInputShape.Integer, CoercionAction.Fail)
            .setCoercion(CoercionInputShape.Float, CoercionAction.Fail)
            .setCoercion(CoercionInputShape.Boolean, CoercionAction.Fail);
        return mapper;
    }

    private static WorkflowDefinition toDomain(DefinitionJson json) {
        if (json == null || json.tasks() == null) {
            return new WorkflowDefinition(List.of());
        }
        List<ValidationError> nullElements = new ArrayList<>();
        for (int i = 0; i < json.tasks().size(); i++) {
            TaskJson task = json.tasks().get(i);
            if (task == null) {
                nullElements.add(new ValidationError("tasks[" + i + "]", ValidationErrorCode.REQUIRED, "task must not be null"));
            } else if (task.dependsOn() != null) {
                for (int j = 0; j < task.dependsOn().size(); j++) {
                    if (task.dependsOn().get(j) == null) {
                        nullElements.add(new ValidationError("tasks[" + i + "].dependsOn[" + j + "]",
                            ValidationErrorCode.REQUIRED, "dependency must not be null"));
                    }
                }
            }
        }
        if (!nullElements.isEmpty()) {
            throw new InvalidWorkflowDefinitionException(nullElements);
        }
        return new WorkflowDefinition(json.tasks().stream().map(WorkflowDefinitionParser::toDomain).toList());
    }

    private static TaskDefinition toDomain(TaskJson t) {
        return new TaskDefinition(t.key(), t.type(), t.config(), t.dependsOn(), millis(t.timeoutMs()), t.priority(),
            t.retry() == null ? null : toDomain(t.retry()));
    }

    private static RetryPolicySpec toDomain(RetryJson r) {
        return new RetryPolicySpec(
            r.maxAttempts() == null ? RetryPolicySpec.DEFAULT_MAX_ATTEMPTS : r.maxAttempts(),
            r.backoff(),
            millis(r.initialDelayMs()),
            millis(r.maxDelayMs()),
            r.multiplier() == null ? RetryPolicySpec.DEFAULT_MULTIPLIER : r.multiplier(),
            r.jitter() == null ? RetryPolicySpec.DEFAULT_JITTER : r.jitter());
    }

    private static TaskJson toDto(TaskDefinition t) {
        RetryPolicySpec r = t.retry();
        return new TaskJson(t.key(), t.type(), t.config(), t.dependsOn(), t.timeout().toMillis(), t.priority(),
            new RetryJson(r.maxAttempts(), r.backoff(), r.initialDelay().toMillis(), r.maxDelay().toMillis(),
                r.multiplier(), r.jitter()));
    }

    private static Duration millis(Long value) {
        return value == null ? null : Duration.ofMillis(value);
    }

    /** Phase 1, reading text into a tree: the only located syntax error is a duplicate field. */
    private static ValidationError syntaxError(JsonProcessingException e) {
        // Jackson has no dedicated type for this; the DUPLICATE_FIELD tests guard against a wording change on upgrade.
        if (e instanceof StreamReadException read && read.getMessage().startsWith("Duplicate field")) {
            return new ValidationError(path(read), ValidationErrorCode.DUPLICATE_FIELD, "field appears more than once");
        }
        return malformed().errors().get(0);
    }

    /** Phase 2, binding the tree to records: every failure is about one located value. */
    private static ValidationError bindingError(JsonProcessingException e) {
        if (e instanceof UnrecognizedPropertyException unknown) {
            return new ValidationError(path(unknown), ValidationErrorCode.UNKNOWN_FIELD,
                "unknown field '" + unknown.getPropertyName() + "'");
        }
        String path = e instanceof JsonMappingException mapping ? path(mapping) : "";
        return new ValidationError(path, ValidationErrorCode.INVALID_VALUE,
            "value has the wrong type, is out of range, or is not one of the allowed values");
    }

    private static InvalidWorkflowDefinitionException malformed() {
        return new InvalidWorkflowDefinitionException(List.of(
            new ValidationError("", ValidationErrorCode.MALFORMED_JSON, "request body is not valid JSON")));
    }

    /** Jackson's reference path ({@code tasks → [1] → retry → tries}) rendered as {@code tasks[1].retry.tries}. */
    private static String path(JsonMappingException e) {
        StringBuilder path = new StringBuilder();
        for (JsonMappingException.Reference ref : e.getPath()) {
            if (ref.getFieldName() != null) {
                appendField(path, ref.getFieldName());
            } else if (ref.getIndex() >= 0) {
                path.append('[').append(ref.getIndex()).append(']');
            }
        }
        return path.toString();
    }

    /** Stream-level errors have no reference path; the parser's current JSON Pointer ({@code /tasks/0/key}) does. */
    private static String path(StreamReadException e) {
        if (!(e.getProcessor() instanceof JsonParser parser)) {
            return "";
        }
        StringBuilder path = new StringBuilder();
        for (JsonPointer p = parser.getParsingContext().pathAsPointer(); !p.matches(); p = p.tail()) {
            if (p.mayMatchElement()) {
                path.append('[').append(p.getMatchingIndex()).append(']');
            } else {
                appendField(path, p.getMatchingProperty());
            }
        }
        return path.toString();
    }

    private static void appendField(StringBuilder path, String field) {
        if (!path.isEmpty()) {
            path.append('.');
        }
        path.append(field);
    }

    private record DefinitionJson(List<TaskJson> tasks) {
    }

    private record TaskJson(String key, TaskType type, JsonNode config, List<String> dependsOn, Long timeoutMs,
                            Priority priority, RetryJson retry) {
    }

    private record RetryJson(Integer maxAttempts, BackoffType backoff, Long initialDelayMs, Long maxDelayMs,
                             Double multiplier, Boolean jitter) {
    }
}

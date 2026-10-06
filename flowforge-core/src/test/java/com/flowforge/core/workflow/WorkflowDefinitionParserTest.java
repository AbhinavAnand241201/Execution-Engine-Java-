package com.flowforge.core.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.flowforge.core.error.ErrorCode;
import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WorkflowDefinitionParserTest {

    private static final String MONTHLY_REPORT = """
        {
          "tasks": [
            { "key": "fetch-data", "type": "DATABASE",
              "config": { "query": "orders_by_status", "params": { "status": "PAID" } } },
            { "key": "generate-report", "type": "REPORT", "dependsOn": ["fetch-data"],
              "config": { "source": "fetch-data", "groupBy": "customer" } },
            { "key": "notify", "type": "HTTP", "dependsOn": ["generate-report"],
              "priority": "HIGH", "timeoutMs": 10000,
              "retry": { "maxAttempts": 5, "backoff": "EXPONENTIAL", "initialDelayMs": 1000, "maxDelayMs": 30000,
                         "multiplier": 2.0, "jitter": true },
              "config": { "method": "POST", "url": "http://wiremock:8080/api/notify" } }
          ]
        }
        """;

    private final WorkflowDefinitionParser parser = new WorkflowDefinitionParser();

    @Test
    @DisplayName("T01.9 parses the reference example and applies defaults to omitted fields")
    void parsesSpecExample() {
        WorkflowDefinition definition = parser.parse(MONTHLY_REPORT);

        assertThat(definition.tasks()).extracting(TaskDefinition::key)
            .containsExactly("fetch-data", "generate-report", "notify");
        TaskDefinition notify = definition.tasks().get(2);
        assertThat(notify.type()).isEqualTo(TaskType.HTTP);
        assertThat(notify.dependsOn()).containsExactly("generate-report");
        assertThat(notify.priority()).isEqualTo(Priority.HIGH);
        assertThat(notify.timeout()).isEqualTo(Duration.ofSeconds(10));
        assertThat(notify.retry().maxAttempts()).isEqualTo(5);
        assertThat(notify.retry().maxDelay()).isEqualTo(Duration.ofSeconds(30));
        assertThat(notify.config().get("method").asText()).isEqualTo("POST");
        assertThat(definition.tasks().get(0).retry()).isEqualTo(RetryPolicySpec.DEFAULT);
    }

    @Test
    @DisplayName("T01.9 JSON round trip: parse(toJson(d)) equals d")
    void roundTrip() {
        WorkflowDefinition definition = parser.parse(MONTHLY_REPORT);

        assertThat(parser.parse(parser.toJson(definition))).isEqualTo(definition);
    }

    @Test
    @DisplayName("T01.9 partial retry block keeps defaults for the fields it omits")
    void partialRetryUsesDefaults() {
        WorkflowDefinition definition = parser.parse("""
            { "tasks": [ { "key": "a", "type": "DELAY", "retry": { "maxAttempts": 7 } } ] }
            """);

        RetryPolicySpec retry = definition.tasks().get(0).retry();
        assertThat(retry.maxAttempts()).isEqualTo(7);
        assertThat(retry.backoff()).isEqualTo(BackoffType.EXPONENTIAL);
        assertThat(retry.initialDelay()).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("T01.9 unknown field is rejected with its JSON path")
    void unknownFieldRejectedWithPath() {
        String json = """
            { "tasks": [ { "key": "a", "type": "DELAY" },
                         { "key": "b", "type": "DELAY", "retry": { "maxAttempts": 2, "tries": 9 } } ] }
            """;

        assertThatThrownBy(() -> parser.parse(json))
            .isInstanceOfSatisfying(InvalidWorkflowDefinitionException.class, e -> {
                assertThat(e.errorCode()).isEqualTo(ErrorCode.INVALID_WORKFLOW_DEFINITION);
                assertThat(e.errors()).extracting(ValidationError::path, ValidationError::code)
                    .containsExactly(tuple("tasks[1].retry.tries", ValidationErrorCode.UNKNOWN_FIELD));
            });
    }

    @Test
    @DisplayName("T01.extra-4 invalid enum value and malformed JSON are reported with a path")
    void invalidValueAndMalformedJson() {
        assertThatThrownBy(() -> parser.parse("{ \"tasks\": [ { \"key\": \"a\", \"type\": \"EMAIL\" } ] }"))
            .isInstanceOfSatisfying(InvalidWorkflowDefinitionException.class, e ->
                assertThat(e.errors()).extracting(ValidationError::path, ValidationError::code)
                    .containsExactly(tuple("tasks[0].type", ValidationErrorCode.INVALID_VALUE)));

        assertThatThrownBy(() -> parser.parse("{ \"tasks\": [ "))
            .isInstanceOfSatisfying(InvalidWorkflowDefinitionException.class, e ->
                assertThat(e.errors()).extracting(ValidationError::code).containsExactly(ValidationErrorCode.MALFORMED_JSON));
    }

    static Stream<Arguments> strictCases() {
        return Stream.of(
            Arguments.of("null task element",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\" }, null ] }",
                "tasks[1]", ValidationErrorCode.REQUIRED),
            Arguments.of("null dependency element",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"dependsOn\": [\"b\", null] } ] }",
                "tasks[0].dependsOn[1]", ValidationErrorCode.REQUIRED),
            Arguments.of("enum given as a number",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": 0 } ] }",
                "tasks[0].type", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("priority given as a number",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"priority\": 3 } ] }",
                "tasks[0].priority", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("key given as a number",
                "{ \"tasks\": [ { \"key\": 123, \"type\": \"DELAY\" } ] }",
                "tasks[0].key", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("number given as a string",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"timeoutMs\": \"5000\" } ] }",
                "tasks[0].timeoutMs", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("boolean given as a string",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"retry\": { \"jitter\": \"false\" } } ] }",
                "tasks[0].retry.jitter", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("int given as a float",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"retry\": { \"maxAttempts\": 2.5 } } ] }",
                "tasks[0].retry.maxAttempts", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("int overflow",
                "{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"retry\": { \"maxAttempts\": 3000000000 } } ] }",
                "tasks[0].retry.maxAttempts", ValidationErrorCode.INVALID_VALUE),
            Arguments.of("duplicate field",
                "{ \"tasks\": [ { \"key\": \"a\", \"key\": \"b\", \"type\": \"DELAY\" } ] }",
                "tasks[0].key", ValidationErrorCode.DUPLICATE_FIELD),
            Arguments.of("trailing content",
                "{ \"tasks\": [] } { \"x\": 1 }",
                "", ValidationErrorCode.MALFORMED_JSON));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("strictCases")
    @DisplayName("T01.9 strict parsing: bad elements, wrong types, duplicates and trailing content are located errors")
    void strictParsing(String name, String json, String path, ValidationErrorCode code) {
        assertThatThrownBy(() -> parser.parse(json))
            .isInstanceOfSatisfying(InvalidWorkflowDefinitionException.class, e ->
                assertThat(e.errors()).extracting(ValidationError::path, ValidationError::code)
                    .containsExactly(tuple(path, code)));
    }

    @Test
    @DisplayName("T01.9 null input is a malformed-JSON error, not an exception from Jackson")
    void nullInput() {
        assertThatThrownBy(() -> parser.parse((String) null))
            .isInstanceOfSatisfying(InvalidWorkflowDefinitionException.class, e ->
                assertThat(e.errors()).extracting(ValidationError::code).containsExactly(ValidationErrorCode.MALFORMED_JSON));
    }

    @Test
    @DisplayName("T01.7 explicit \"config\": null becomes {} like a missing config")
    void explicitNullConfigBecomesEmptyObject() {
        WorkflowDefinition definition = parser.parse("{ \"tasks\": [ { \"key\": \"a\", \"type\": \"DELAY\", \"config\": null } ] }");

        assertThat(definition.tasks().get(0).config().isObject()).isTrue();
        assertThat(definition.tasks().get(0).config().isEmpty()).isTrue();
    }
}

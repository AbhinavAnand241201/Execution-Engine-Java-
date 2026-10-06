package com.flowforge.core.workflow;

import static com.flowforge.core.workflow.TaskType.DELAY;
import static com.flowforge.core.workflow.TaskType.HTTP;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Duration;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WorkflowDefinitionValidatorTest {

    private final WorkflowDefinitionValidator validator = new WorkflowDefinitionValidator();

    @Test
    @DisplayName("T01.1 a valid 3-task linear definition passes")
    void validLinearDefinitionPasses() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", DELAY)
            .task("b", DELAY).dependsOn("a")
            .task("c", HTTP).dependsOn("b")
            .build();

        assertThat(pathsAndCodes(validator.validate(definition))).isEmpty();
    }

    @Test
    @DisplayName("T01.2 duplicate task keys → DUPLICATE_TASK_KEY at tasks[i].key")
    void duplicateKeys() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", DELAY)
            .task("b", DELAY)
            .task("a", DELAY)
            .build();

        assertThat(pathsAndCodes(validator.validate(definition)))
            .containsExactly(error("tasks[2].key", ValidationErrorCode.DUPLICATE_TASK_KEY));
    }

    @Test
    @DisplayName("T01.3 unknown dependency → UNKNOWN_DEPENDENCY at tasks[i].dependsOn[j]")
    void unknownDependency() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", DELAY)
            .task("b", DELAY).dependsOn("a", "ghost")
            .build();

        assertThat(pathsAndCodes(validator.validate(definition)))
            .containsExactly(error("tasks[1].dependsOn[1]", ValidationErrorCode.UNKNOWN_DEPENDENCY));
    }

    @Test
    @DisplayName("T01.4 self dependency → SELF_DEPENDENCY")
    void selfDependency() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", DELAY).dependsOn("a")
            .build();

        assertThat(pathsAndCodes(validator.validate(definition)))
            .containsExactly(error("tasks[0].dependsOn[0]", ValidationErrorCode.SELF_DEPENDENCY));
    }

    @Test
    @DisplayName("T01.5 a definition with 4 different problems reports all 4 (not fail-fast)")
    void collectsAllErrors() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", DELAY).timeout(Duration.ofMillis(5))
            .task("a", DELAY)
            .task("Bad_Key", DELAY)
            .task("c", DELAY).dependsOn("ghost")
            .build();

        assertThat(pathsAndCodes(validator.validate(definition))).containsExactlyInAnyOrder(
            error("tasks[0].timeoutMs", ValidationErrorCode.OUT_OF_RANGE),
            error("tasks[1].key", ValidationErrorCode.DUPLICATE_TASK_KEY),
            error("tasks[2].key", ValidationErrorCode.INVALID_TASK_KEY),
            error("tasks[3].dependsOn[0]", ValidationErrorCode.UNKNOWN_DEPENDENCY));
    }

    static Stream<Arguments> outOfBounds() {
        return Stream.of(
            Arguments.of("0 tasks", definitionWithTasks(0), "tasks", ValidationErrorCode.TOO_FEW_TASKS),
            Arguments.of("201 tasks", definitionWithTasks(201), "tasks", ValidationErrorCode.TOO_MANY_TASKS),
            Arguments.of("bad key pattern", single("-starts-with-dash", t -> t), "tasks[0].key",
                ValidationErrorCode.INVALID_TASK_KEY),
            Arguments.of("65-char key", single("k".repeat(65), t -> t), "tasks[0].key", ValidationErrorCode.INVALID_TASK_KEY),
            Arguments.of("timeout 99 ms", single("a", t -> t.timeout(Duration.ofMillis(99))), "tasks[0].timeoutMs",
                ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("timeout 3 600 001 ms", single("a", t -> t.timeout(Duration.ofMillis(3_600_001))),
                "tasks[0].timeoutMs", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("maxAttempts 0", single("a", t -> t.retry(retry(0, BackoffType.EXPONENTIAL, 1_000, 60_000, 2.0))),
                "tasks[0].retry.maxAttempts", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("maxAttempts 11", single("a", t -> t.retry(retry(11, BackoffType.EXPONENTIAL, 1_000, 60_000, 2.0))),
                "tasks[0].retry.maxAttempts", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("maxDelay < initialDelay", single("a", t -> t.retry(retry(3, BackoffType.FIXED, 5_000, 1_000, 2.0))),
                "tasks[0].retry.maxDelayMs", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("initialDelay > 600 000", single("a", t -> t.retry(retry(3, BackoffType.FIXED, 600_001, 3_600_000, 2.0))),
                "tasks[0].retry.initialDelayMs", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("multiplier 0.5", single("a", t -> t.retry(retry(3, BackoffType.EXPONENTIAL, 1_000, 60_000, 0.5))),
                "tasks[0].retry.multiplier", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("multiplier 10.01", single("a", t -> t.retry(retry(3, BackoffType.EXPONENTIAL, 1_000, 60_000, 10.01))),
                "tasks[0].retry.multiplier", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("multiplier NaN even for FIXED", single("a", t -> t.retry(retry(3, BackoffType.FIXED, 1_000, 60_000, Double.NaN))),
                "tasks[0].retry.multiplier", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("timeout -1 ms", single("a", t -> t.timeout(Duration.ofMillis(-1))), "tasks[0].timeoutMs",
                ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("initialDelay -1 ms", single("a", t -> t.retry(retry(3, BackoffType.FIXED, -1, 60_000, 2.0))),
                "tasks[0].retry.initialDelayMs", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("maxDelay 3 600 001 ms", single("a", t -> t.retry(retry(3, BackoffType.FIXED, 1_000, 3_600_001, 2.0))),
                "tasks[0].retry.maxDelayMs", ValidationErrorCode.OUT_OF_RANGE),
            Arguments.of("config is not an object", single("a", t -> t.config(JsonNodeFactory.instance.arrayNode().add(1))),
                "tasks[0].config", ValidationErrorCode.INVALID_TASK_CONFIG));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("outOfBounds")
    @DisplayName("T01.6 bounds are enforced")
    void boundsAreEnforced(String name, WorkflowDefinition definition, String path, ValidationErrorCode code) {
        assertThat(pathsAndCodes(validator.validate(definition))).containsExactly(error(path, code));
    }

    @Test
    @DisplayName("T01.6 bounds: 200 tasks, 64-char key, 100 ms and 3 600 000 ms timeouts are accepted")
    void boundaryValuesAreAccepted() {
        assertThat(pathsAndCodes(validator.validate(definitionWithTasks(200)))).isEmpty();
        assertThat(pathsAndCodes(validator.validate(single("k".repeat(64), t -> t.timeout(Duration.ofMillis(100)))))).isEmpty();
        assertThat(pathsAndCodes(validator.validate(single("a", t -> t.timeout(Duration.ofMillis(3_600_000)))))).isEmpty();
        assertThat(pathsAndCodes(validator.validate(single("a", t -> t.retry(retry(1, BackoffType.EXPONENTIAL, 0, 0, 1.0)))))).isEmpty();
        assertThat(pathsAndCodes(validator.validate(
            single("a", t -> t.retry(retry(10, BackoffType.EXPONENTIAL, 600_000, 3_600_000, 10.0)))))).isEmpty();
    }

    @Test
    @DisplayName("T01.6 an invalid initialDelay is reported once, not also blamed on the default maxDelay")
    void invalidInitialDelayReportedOnce() {
        assertThat(pathsAndCodes(validator.validate(single("a", t -> t.retry(retry(3, BackoffType.FIXED, 700_000, 60_000, 2.0))))))
            .containsExactly(error("tasks[0].retry.initialDelayMs", ValidationErrorCode.OUT_OF_RANGE));
    }

    @Test
    @DisplayName("T01.6 multiplier is only checked for EXPONENTIAL backoff")
    void multiplierIgnoredForFixedBackoff() {
        assertThat(pathsAndCodes(validator.validate(single("a", t -> t.retry(retry(3, BackoffType.FIXED, 1_000, 60_000, 0.5)))))).isEmpty();
    }

    @Test
    @DisplayName("T01.extra-1 duplicate dependency, missing key and missing type are reported")
    void duplicateDependencyAndMissingType() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", DELAY)
            .task("b", null).dependsOn("a", "a")
            .task(null, DELAY)
            .build();

        assertThat(pathsAndCodes(validator.validate(definition))).containsExactlyInAnyOrder(
            error("tasks[2].key", ValidationErrorCode.REQUIRED),
            error("tasks[1].type", ValidationErrorCode.REQUIRED),
            error("tasks[1].dependsOn[1]", ValidationErrorCode.DUPLICATE_DEPENDENCY));
    }

    @Test
    @DisplayName("T01.extra-2 config errors from the pluggable ConfigValidator are prefixed with the task's config path")
    void configValidatorHook() {
        ConfigValidator rejectAll = (type, config) ->
            List.of(new ValidationError("durationMs", ValidationErrorCode.INVALID_TASK_CONFIG, "must be >= 0"));
        WorkflowDefinition definition = WorkflowDefinition.builder().task("a", DELAY).task("b", DELAY).build();

        assertThat(pathsAndCodes(new WorkflowDefinitionValidator(rejectAll, 200).validate(definition))).containsExactly(
            error("tasks[0].config.durationMs", ValidationErrorCode.INVALID_TASK_CONFIG),
            error("tasks[1].config.durationMs", ValidationErrorCode.INVALID_TASK_CONFIG));
    }

    private static WorkflowDefinition definitionWithTasks(int count) {
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        for (int i = 0; i < count; i++) {
            builder.task("t" + i, DELAY);
        }
        return builder.build();
    }

    private static WorkflowDefinition single(String key, UnaryOperator<WorkflowDefinition.Builder> customize) {
        return customize.apply(WorkflowDefinition.builder().task(key, DELAY)).build();
    }

    private static RetryPolicySpec retry(int maxAttempts, BackoffType backoff, long initialMs, long maxMs, double multiplier) {
        return new RetryPolicySpec(maxAttempts, backoff, Duration.ofMillis(initialMs), Duration.ofMillis(maxMs), multiplier, true);
    }

    /** Assertions match on path and code; messages are for humans and may be reworded. */
    private static List<String> pathsAndCodes(List<ValidationError> errors) {
        return errors.stream().map(e -> e.path() + " " + e.code()).toList();
    }

    private static String error(String path, ValidationErrorCode code) {
        return path + " " + code;
    }
}

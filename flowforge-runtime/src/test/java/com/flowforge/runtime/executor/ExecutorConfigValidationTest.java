package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.Contexts.json;
import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.executor.DelayTaskExecutor;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.workflow.ValidationErrorCode;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.DisplayName;

class ExecutorConfigValidationTest {

    private static final TaskExecutor HTTP = new HttpTaskExecutor(
        new HttpExecutorSettings(Set.of("localhost"), Duration.ofSeconds(1), 16_384), Contexts.JSON, Clock.systemUTC());
    private static final TaskExecutor DATABASE = new DatabaseTaskExecutor(null,
        new DatabaseExecutorSettings(Map.of("q", "SELECT :a"), Duration.ofSeconds(1), 10), Contexts.JSON);
    private static final TaskExecutor REPORT = new ReportTaskExecutor(Contexts.JSON);
    private static final TaskExecutor DELAY = new DelayTaskExecutor();

    static Stream<Arguments> invalidConfigs() {
        return Stream.of(
            Arguments.of(HTTP, "{}", "method"),
            Arguments.of(HTTP, "{\"method\":\"GET\"}", "url"),
            Arguments.of(HTTP, "{\"url\":\"http://localhost/x\"}", "method"),
            Arguments.of(HTTP, "{\"method\":\"TRACE\",\"url\":\"http://localhost/x\"}", "method"),
            Arguments.of(HTTP, "{\"method\":\"GET\",\"url\":\"http://localhost/x\",\"expectedStatus\":[300,200]}",
                "expectedStatus"),
            Arguments.of(HTTP, "{\"method\":\"GET\",\"url\":\"http://localhost/x\",\"timeoutMs\":0}", "timeoutMs"),
            Arguments.of(HTTP, "{\"method\":\"GET\",\"url\":\"http://localhost/x\",\"extra\":1}", "extra"),
            Arguments.of(DATABASE, "{}", "query"),
            Arguments.of(DATABASE, "{\"query\":\"q\"}", "params.a"),
            Arguments.of(DATABASE, "{\"query\":\"q\",\"params\":[]}", "params"),
            Arguments.of(REPORT, "{\"metrics\":[{\"field\":\"a\",\"fn\":\"sum\"}]}", "source"),
            Arguments.of(REPORT, "{\"source\":\"s\"}", "metrics"),
            Arguments.of(REPORT, "{\"source\":\"s\",\"metrics\":[{\"field\":\"a\"}]}", "metrics[0].fn"),
            Arguments.of(REPORT, "{\"source\":\"s\",\"metrics\":[{\"fn\":\"median\",\"field\":\"a\"}]}", "metrics[0].fn"),
            Arguments.of(REPORT, "{\"source\":\"s\",\"metrics\":[{\"fn\":\"sum\"}]}", "metrics[0].field"),
            Arguments.of(DELAY, "{}", "durationMs"));
    }

    @ParameterizedTest(name = "{0} {1} → error at {2}")
    @MethodSource("invalidConfigs")
    @DisplayName("T10.10 every executor's validateConfig rejects missing or invalid required fields")
    void rejectsMissingFields(TaskExecutor executor, String config, String path) {
        assertThat(executor.validateConfig(json(config)))
            .anySatisfy(error -> {
                assertThat(error.path()).isEqualTo(path);
                assertThat(error.code()).isEqualTo(ValidationErrorCode.INVALID_TASK_CONFIG);
            });
    }

    static Stream<Arguments> validConfigs() {
        return Stream.of(
            Arguments.of(HTTP, "{\"method\":\"POST\",\"url\":\"https://localhost:8443/x\",\"body\":{\"a\":1},"
                + "\"headers\":{\"X-A\":\"b\"},\"expectedStatus\":[200,204],\"timeoutMs\":1000}"),
            Arguments.of(DATABASE, "{\"query\":\"q\",\"params\":{\"a\":1}}"),
            Arguments.of(REPORT, "{\"source\":\"s\",\"groupBy\":\"g\",\"metrics\":[{\"field\":\"a\",\"fn\":\"avg\"}]}"),
            Arguments.of(DELAY, "{\"durationMs\":10}"));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("validConfigs")
    @DisplayName("T10.10 complete configs pass validation")
    void acceptsValid(TaskExecutor executor, String config) {
        assertThat(executor.validateConfig(json(config))).isEmpty();
    }
}

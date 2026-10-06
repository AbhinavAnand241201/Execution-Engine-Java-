package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.Contexts.json;
import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.executor.TaskResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OutputGuardTest {

    private final OutputGuard guard = new OutputGuard(65_536);

    @Test
    @DisplayName("T10.6 output over 64 KB → non-retryable OUTPUT_TOO_LARGE; at the limit and other results pass through")
    void outputLimit() {
        TaskResult big = TaskResult.success(Contexts.JSON.createObjectNode().put("blob", "z".repeat(70_000)));
        assertThat(guard.check(big)).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.errorCode()).isEqualTo("OUTPUT_TOO_LARGE");
            assertThat(f.retryable()).isFalse();
        });

        // {"b":"..."} is 8 bytes of structure.
        TaskResult exact = TaskResult.success(Contexts.JSON.createObjectNode().put("b", "z".repeat(65_536 - 8)));
        assertThat(guard.check(exact)).isSameAs(exact);
        TaskResult failure = TaskResult.failure("X", "y", true);
        assertThat(guard.check(failure)).isSameAs(failure);
        assertThat(guard.check(TaskResult.success(json("{\"a\":1}")))).isInstanceOf(TaskResult.Success.class);
    }
}

package com.flowforge.runtime.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.executor.TaskResult;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DatabaseFailureClassificationTest {

    @ParameterizedTest(name = "SQLSTATE {0}")
    @ValueSource(strings = {"57014", "08006", "08001", "57P01", "57P02", "57P03", "53300", "53200", "40001", "40P01"})
    @DisplayName("T10.extra-6 timeouts, lost connections, restarts, overload, serialization failures and deadlocks are "
        + "retryable")
    void retryable(String state) {
        assertThat(DatabaseTaskExecutor.classify(new SQLException("x", state)))
            .isInstanceOfSatisfying(TaskResult.Failure.class, f -> assertThat(f.retryable()).isTrue());
    }

    @ParameterizedTest(name = "SQLSTATE {0}")
    @ValueSource(strings = {"25006", "42601", "42P01", "22P02", "23505"})
    @DisplayName("T10.extra-6 read-only violations, bad SQL, missing tables and bad input are permanent")
    void permanent(String state) {
        assertThat(DatabaseTaskExecutor.classify(new SQLException("x", state)))
            .isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
                assertThat(f.retryable()).isFalse();
                assertThat(f.errorCode()).isEqualTo("DATABASE_ERROR");
            });
    }

    @Test
    @DisplayName("T10.extra-6 a pool timeout (transient exception without SQLSTATE) is retryable")
    void poolTimeout() {
        assertThat(DatabaseTaskExecutor.classify(new SQLTransientConnectionException("pool timeout")))
            .isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
                assertThat(f.retryable()).isTrue();
                assertThat(f.errorCode()).isEqualTo("DATABASE_UNAVAILABLE");
            });
    }
}

package com.flowforge.core.engine;

import java.time.Duration;
import java.util.Objects;

/**
 * @param parallelism   at most this many tasks run at the same time, across all executions of the engine
 * @param shutdownGrace how long {@code close()} waits for running tasks to notice cancellation before interrupting
 */
public record LocalEngineOptions(ExecutionMode mode, int parallelism, Duration shutdownGrace) {

    public static final Duration DEFAULT_SHUTDOWN_GRACE = Duration.ofSeconds(1);

    public LocalEngineOptions {
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(shutdownGrace, "shutdownGrace");
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be >= 1: " + parallelism);
        }
    }

    public LocalEngineOptions(ExecutionMode mode, int parallelism) {
        this(mode, parallelism, DEFAULT_SHUTDOWN_GRACE);
    }
}

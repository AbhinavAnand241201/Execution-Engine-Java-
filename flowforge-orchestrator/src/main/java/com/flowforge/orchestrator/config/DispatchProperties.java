package com.flowforge.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.dispatch.*}. {@code jobEnabled=false} stops the background dispatch loop (tests that drive
 * dispatch by hand). Kafka-mode capacity comes from the worker registry, not from configuration.
 */
@ConfigurationProperties("flowforge.dispatch")
public record DispatchProperties(Mode mode, Duration pollInterval, int batchSize, boolean jobEnabled) {

    /** {@code local}: tasks run in this process; {@code kafka}: commands go to workers. */
    public enum Mode { LOCAL, KAFKA }
}

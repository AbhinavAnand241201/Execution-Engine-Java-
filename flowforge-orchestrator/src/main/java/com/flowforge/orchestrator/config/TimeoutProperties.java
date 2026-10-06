package com.flowforge.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.timeout.*}: how often the sweeper runs, and how long past a task's deadline it waits for
 * the worker's own TIMED_OUT result before declaring the attempt timed out itself.
 */
@ConfigurationProperties("flowforge.timeout")
public record TimeoutProperties(Duration sweepInterval, Duration grace) {
}

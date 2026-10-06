package com.flowforge.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.limits.*}: size limits that protect the database and the workers. {@code requestMaxBytes}
 * caps any request body before it is parsed; the per-field limits are the exact ones.
 */
@ConfigurationProperties("flowforge.limits")
public record LimitsProperties(int outputMaxBytes, int inputMaxBytes, int maxTasksPerWorkflow, long requestMaxBytes) {
}

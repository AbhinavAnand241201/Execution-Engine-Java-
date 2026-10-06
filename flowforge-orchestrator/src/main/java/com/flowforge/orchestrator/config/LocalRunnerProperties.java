package com.flowforge.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.local-runner.*}: the in-process task runner of local mode. */
@ConfigurationProperties("flowforge.local-runner")
public record LocalRunnerProperties(int maxConcurrency) {
}

package com.flowforge.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.limits.*}: the worker only needs the task output cap (64 KB). */
@ConfigurationProperties("flowforge.limits")
public record LimitsProperties(int outputMaxBytes) {
}

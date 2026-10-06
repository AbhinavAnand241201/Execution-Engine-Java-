package com.flowforge.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.jobs.*}: switches the background engine jobs (retry promoter, timeout sweeper and later ones) on
 * or off as a group. Tests turn them off so a job can't touch another test's rows, and drive passes by hand.
 */
@ConfigurationProperties("flowforge.jobs")
public record JobsProperties(boolean enabled, int batchSize) {
}

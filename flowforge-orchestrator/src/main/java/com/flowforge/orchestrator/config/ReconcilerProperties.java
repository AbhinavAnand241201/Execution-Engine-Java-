package com.flowforge.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.reconciler.*}: how often stale QUEUED tasks are looked for, and what "stale" means. */
@ConfigurationProperties("flowforge.reconciler")
public record ReconcilerProperties(Duration interval, Duration staleQueuedAfter) {
}

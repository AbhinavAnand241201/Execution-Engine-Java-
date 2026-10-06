package com.flowforge.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.retry.*}: how often due retries are promoted to READY. */
@ConfigurationProperties("flowforge.retry")
public record RetryProperties(Duration promoterInterval) {
}

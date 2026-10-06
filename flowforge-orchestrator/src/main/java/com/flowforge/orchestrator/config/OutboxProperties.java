package com.flowforge.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.outbox.*}: relay pace and batch, how long one send may take, how long rows are kept. */
@ConfigurationProperties("flowforge.outbox")
public record OutboxProperties(Duration relayInterval, int batchSize, Duration sendTimeout, Duration retention,
                               Duration janitorInterval) {
}

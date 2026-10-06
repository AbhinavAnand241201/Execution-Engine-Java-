package com.flowforge.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.workers.*}: a worker without a heartbeat for {@code heartbeatTtl} (and registered longer
 * ago than that) is declared OFFLINE by the monitor, which runs every {@code monitorInterval}.
 */
@ConfigurationProperties("flowforge.workers")
public record WorkersProperties(Duration heartbeatTtl, Duration monitorInterval, Duration queueDepthInterval) {
}

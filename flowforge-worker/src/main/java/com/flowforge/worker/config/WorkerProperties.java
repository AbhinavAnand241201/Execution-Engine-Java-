package com.flowforge.worker.config;

import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.runner.ThreadMode;
import java.time.Duration;
import java.util.Set;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.worker.*}. {@code name} is the stable name (default: hostname); the worker id is
 * {@code <name>-<8 hex>}, new on every process start, so a restarted worker is a new incarnation.
 * {@code exitWhenDeclaredOffline}: if the orchestrator declared this incarnation OFFLINE, exit (status 70) so the
 * container restarts as a new one; tests that run workers in-process turn it off (the worker just stops working).
 */
@ConfigurationProperties("flowforge.worker")
public record WorkerProperties(String name, Set<TaskType> capabilities, int maxConcurrency, ThreadMode threadMode,
                               int listenerConcurrency, Duration shutdownGrace, Duration heartbeatInterval,
                               Duration heartbeatTtl, boolean exitWhenDeclaredOffline) {

    public WorkerProperties {
        capabilities = capabilities == null ? Set.of() : Set.copyOf(capabilities);
        if (heartbeatInterval != null && heartbeatTtl != null
            && heartbeatInterval.multipliedBy(2).compareTo(heartbeatTtl) > 0) {
            throw new IllegalArgumentException("flowforge.worker.heartbeat-interval must be at most half the "
                + "heartbeat-ttl, or one late beat makes the worker look dead");
        }
    }
}

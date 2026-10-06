package com.flowforge.worker.lifecycle;

import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.runner.TaskRunner;
import com.flowforge.worker.messaging.CommandConsumers;
import com.flowforge.worker.registry.HeartbeatService;
import com.flowforge.worker.registry.WorkerFreeze;
import com.flowforge.worker.registry.WorkerRegistration;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * The order a worker starts and stops in. Start: register (row ACTIVE + WorkerRegistered) → heartbeat →
 * consume, so the orchestrator knows the worker before any command can reach it. Stop (SIGTERM / context close) is
 * the reverse: stop consuming → give in-flight attempts {@code shutdownGrace}, then interrupt the rest and report them
 * WORKER_SHUTDOWN (retryable: retried elsewhere at once) → mark OFFLINE → delete the heartbeat key.
 *
 * <p>Stopped while frozen (the crash simulation) nothing graceful happens: results are dropped, the row stays ACTIVE
 * and the key expires on its own, exactly as if the process had been killed.
 */
public class WorkerLifecycle implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerLifecycle.class);

    /** How the worker describes itself when registering. */
    public record Registration(String workerId, String name, String hostname, int maxConcurrency) {
    }

    private final WorkerRegistration registration;
    private final HeartbeatService heartbeat;
    private final CommandConsumers consumers;
    private final TaskRunner runner;
    private final WorkerFreeze freeze;
    private final Clock clock;
    private final Supplier<Registration> self;
    private final Set<TaskType> capabilities;
    private final Duration shutdownGrace;
    private volatile boolean running;

    public WorkerLifecycle(WorkerRegistration registration, HeartbeatService heartbeat, CommandConsumers consumers,
                           TaskRunner runner, WorkerFreeze freeze, Clock clock, Supplier<Registration> self,
                           Set<TaskType> capabilities, Duration shutdownGrace) {
        this.registration = registration;
        this.heartbeat = heartbeat;
        this.consumers = consumers;
        this.runner = runner;
        this.freeze = freeze;
        this.clock = clock;
        this.self = self;
        this.capabilities = capabilities;
        this.shutdownGrace = shutdownGrace;
    }

    @Override
    public void start() {
        Registration me = self.get();
        registration.register(me.workerId(), me.name(), me.hostname(), capabilities, me.maxConcurrency(),
            clock.instant());
        heartbeat.start();
        consumers.start();
        running = true;
        log.info("worker started workerId={} capabilities={}", me.workerId(), capabilities);
    }

    @Override
    public void stop() {
        String workerId = self.get().workerId();
        if (freeze.isFrozen()) {
            freeze.crash();
            consumers.stop();
            heartbeat.stop(false);
            try {
                runner.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            running = false;
            log.warn("worker stopped while frozen (simulated crash) workerId={}", workerId);
            return;
        }
        consumers.stop();
        try {
            boolean clean = runner.shutdown(shutdownGrace);
            log.info("worker drained workerId={} allFinishedInGrace={}", workerId, clean);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        heartbeat.stop(true);
        registration.markOffline(workerId, clock.instant());
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Last to start, first to stop (after the Kafka producer it reports results through). */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}

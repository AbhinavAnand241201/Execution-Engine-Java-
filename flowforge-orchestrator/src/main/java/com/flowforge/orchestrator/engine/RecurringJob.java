package com.flowforge.orchestrator.engine;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs one background job pass every {@code interval} on its own named thread, with a fixed delay so a
 * slow pass never overlaps the next one. A failing pass (database blip) is logged and retried on the next tick; it
 * never kills the schedule. Every job is safe to run concurrently with itself on another orchestrator.
 */
public class RecurringJob implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RecurringJob.class);

    private final String name;
    private final Runnable pass;
    private final Duration interval;
    private volatile ScheduledExecutorService scheduler;

    public RecurringJob(String name, Duration interval, Runnable pass) {
        this.name = name;
        this.pass = pass;
        this.interval = interval;
    }

    @Override
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("ff-job-" + name).daemon()
            .factory());
        scheduler.scheduleWithFixedDelay(this::runOnce, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void runOnce() {
        try {
            pass.run();
        } catch (RuntimeException e) {
            log.warn("job pass failed job={} error={}", name, e.toString());
        }
    }

    @Override
    public void stop() {
        ScheduledExecutorService current = scheduler;
        if (current != null) {
            current.shutdownNow();
            try {
                current.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        scheduler = null;
    }

    @Override
    public boolean isRunning() {
        return scheduler != null;
    }

    /** Same phase as the dispatch loop: start after the runner, stop before it. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}

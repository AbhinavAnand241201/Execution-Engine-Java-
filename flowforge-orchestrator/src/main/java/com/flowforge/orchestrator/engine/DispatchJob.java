package com.flowforge.orchestrator.engine;

import com.flowforge.orchestrator.application.ReadyTaskSignal;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs {@link DispatchService#dispatchOnce()} in a loop on one thread: every {@code pollInterval}, or at once when
 * {@link ReadyTaskSignal} says new tasks are READY (a finished task frees capacity and usually makes others READY,
 * so a busy system is rarely waiting out the interval).
 * One loop per orchestrator; several orchestrators are still safe because every dispatch is a locked conditional
 * update (adds a lock only to avoid wasted work).
 */
public class DispatchJob implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DispatchJob.class);

    private final DispatchService dispatch;
    private final ReadyTaskSignal readyTasks;
    private final Duration pollInterval;
    private volatile Thread thread;
    private volatile boolean running;

    public DispatchJob(DispatchService dispatch, ReadyTaskSignal readyTasks, Duration pollInterval) {
        this.dispatch = dispatch;
        this.readyTasks = readyTasks;
        this.pollInterval = pollInterval;
    }

    @Override
    public void start() {
        running = true;
        thread = Thread.ofPlatform().name("ff-dispatch").daemon().start(this::loop);
    }

    private void loop() {
        while (running) {
            try {
                dispatch.dispatchOnce();
                readyTasks.await(pollInterval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // Database blip or a bad row: log, back off one interval, keep going.
                log.warn("dispatch pass failed error={}", e.toString());
                try {
                    Thread.sleep(pollInterval.toMillis());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    @Override
    public void stop() {
        running = false;
        Thread current = thread;
        if (current != null) {
            current.interrupt();
            try {
                current.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    /** Start after the runner and stop before it, so no command is sent to a runner that's gone. */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }
}

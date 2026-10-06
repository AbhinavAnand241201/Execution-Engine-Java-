package com.flowforge.orchestrator.application;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * "Tasks just became READY" — a wake-up for the dispatch loop, so it doesn't wait out its poll interval after a
 * commit that made work available. Purely a latency optimisation: the loop also polls, so a lost signal only delays
 * dispatch. Signal after commit, never before (the loop must be able to see the rows).
 */
public final class ReadyTaskSignal {

    private final Semaphore pending = new Semaphore(0);

    public void signal() {
        if (pending.availablePermits() == 0) {
            pending.release();
        }
    }

    /** Waits up to {@code timeout} for a signal; returns early (and consumes all pending signals) if one arrives. */
    public void await(Duration timeout) throws InterruptedException {
        if (pending.tryAcquire(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            pending.drainPermits();
        }
    }
}

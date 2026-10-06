package com.flowforge.worker.registry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Simulates a frozen or crashed process (used by system tests and chaos): while frozen the worker stops
 * heartbeating and consuming, and finished attempts can't report (publishing blocks). Unfreezing turns it into a
 * zombie: its old results go out late and must be rejected as stale. A worker stopped while frozen behaves like a
 * crash: no graceful shutdown, results dropped.
 */
public class WorkerFreeze {

    private volatile CountDownLatch thawed = new CountDownLatch(0);
    private volatile boolean frozen;
    private volatile boolean crashed;

    public void freeze() {
        thawed = new CountDownLatch(1);
        frozen = true;
    }

    public void unfreeze() {
        frozen = false;
        thawed.countDown();
    }

    /** Stopped while frozen: wake any blocked publisher so it can give up. */
    public void crash() {
        crashed = true;
        thawed.countDown();
    }

    public boolean isFrozen() {
        return frozen;
    }

    /** Blocks while frozen; throws if the process "crashed" meanwhile (the result is never sent). */
    public void awaitThawed() throws InterruptedException {
        while (frozen && !crashed) {
            thawed.await(1, TimeUnit.SECONDS);
        }
        if (crashed) {
            throw new IllegalStateException("worker crashed: result not sent");
        }
    }
}

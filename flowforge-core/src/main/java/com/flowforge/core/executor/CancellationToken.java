package com.flowforge.core.executor;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thread-safe "please stop" flag shared between whoever may cancel an attempt (user cancel, timeout, shutdown)
 * and the executor running it. Cancellation in Java is cooperative: the executor must check this (or be
 * interrupted); nothing can safely kill a running thread from outside.
 */
public final class CancellationToken {

    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final List<Runnable> callbacks = new CopyOnWriteArrayList<>();

    /** Cancels once; later calls do nothing. Callbacks run on the calling thread. */
    public void cancel() {
        if (cancelled.compareAndSet(false, true)) {
            callbacks.forEach(Runnable::run);
        }
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public void throwIfCancelled() {
        if (isCancelled()) {
            throw new CancellationException("cancelled");
        }
    }

    /**
     * Runs {@code callback} when cancelled, or right away if already cancelled. Callbacks run on the thread that
     * cancels (often the engine's coordinator), so they must be fast and must not block. A callback registered while
     * {@link #cancel()} is running may run twice; callbacks must be idempotent.
     */
    public void onCancel(Runnable callback) {
        callbacks.add(callback);
        if (isCancelled()) {
            callback.run();
            callbacks.remove(callback);
        }
    }
}

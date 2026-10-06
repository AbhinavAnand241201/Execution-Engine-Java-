package com.flowforge.core.engine;

import java.time.Duration;

/** Runs prioritized tasks on some kind of thread, at most {@code parallelism} at a time. */
interface ExecutionStrategy {

    void submit(PrioritizedTask task);

    /**
     * Tasks running right now, for leak checks (virtual threads are invisible to {@code getAllStackTraces}). Exact
     * for VIRTUAL; approximate for PLATFORM ({@code ThreadPoolExecutor.getActiveCount()}).
     */
    int activeWorkers();

    /** Stops taking work, waits up to {@code grace} for running tasks, then interrupts what's left. */
    void shutdown(Duration grace);

    static ExecutionStrategy create(ExecutionMode mode, int parallelism) {
        return switch (mode) {
            case PLATFORM -> new PlatformStrategy(parallelism);
            case VIRTUAL -> new VirtualStrategy(parallelism);
        };
    }
}

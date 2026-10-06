package com.flowforge.core.state;

/**
 * What happened to one attempt of a task (one row in {@code task_attempts}). Kept apart from {@link TaskStatus}:
 * "this attempt failed" is a fact about the past, while the task itself moves on to RETRYING or
 * FAILED_PERMANENTLY in the same transaction.
 */
public enum AttemptOutcome {
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    WORKER_LOST,
    WORKER_SHUTDOWN,
    CANCELLED;

    /**
     * True for outcomes caused by the infrastructure rather than the task's own logic: a retry may well succeed, so
     * these are retried regardless of what the executor would say. FAILED is retryable only if the executor says so.
     */
    public boolean isAlwaysRetryable() {
        return this == TIMED_OUT || this == WORKER_LOST || this == WORKER_SHUTDOWN;
    }
}

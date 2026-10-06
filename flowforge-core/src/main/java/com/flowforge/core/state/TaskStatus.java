package com.flowforge.core.state;

/**
 * Where a task execution is in its life. A task never rests in a "failed" state between attempts: a
 * failed attempt moves it straight to RETRYING or FAILED_PERMANENTLY, which keeps this machine small. What happened
 * to each individual attempt is recorded separately as an attempt outcome.
 */
public enum TaskStatus {
    PENDING,
    READY,
    QUEUED,
    RUNNING,
    RETRYING,
    SUCCEEDED,
    FAILED_PERMANENTLY,
    CANCELLED
}

package com.flowforge.core.state;

/**
 * Where a workflow execution is in its life. CANCELLING exists because cancellation is cooperative:
 * running tasks must be told to stop and report back before the execution can be called CANCELLED.
 */
public enum WorkflowStatus {
    CREATED,
    RUNNING,
    PAUSED,
    CANCELLING,
    COMPLETED,
    FAILED,
    CANCELLED
}

package com.flowforge.core.state;

import static com.flowforge.core.state.TaskStatus.CANCELLED;
import static com.flowforge.core.state.TaskStatus.FAILED_PERMANENTLY;
import static com.flowforge.core.state.TaskStatus.PENDING;
import static com.flowforge.core.state.TaskStatus.QUEUED;
import static com.flowforge.core.state.TaskStatus.READY;
import static com.flowforge.core.state.TaskStatus.RETRYING;
import static com.flowforge.core.state.TaskStatus.RUNNING;
import static com.flowforge.core.state.TaskStatus.SUCCEEDED;

/** The task lifecycle as one table. Every task status change in FlowForge is checked against it. */
public final class TaskStateMachine {

    public static final StateMachine<TaskStatus> INSTANCE = StateMachine.builder("task execution", TaskStatus.class)
        .allow(PENDING, READY, CANCELLED)                           // dependencies satisfied, or workflow cancelled
        .allow(READY, QUEUED, CANCELLED)                            // dispatched (attempt + 1)
        .allow(QUEUED, RUNNING, CANCELLED)                          // claimed by a worker
        .allow(RUNNING, SUCCEEDED, RETRYING, FAILED_PERMANENTLY, CANCELLED)
        .allow(RETRYING, READY, CANCELLED)                          // backoff elapsed
        .allow(FAILED_PERMANENTLY, READY)                           // DLQ replay or execution retry only
        .build();

    private TaskStateMachine() {
    }
}

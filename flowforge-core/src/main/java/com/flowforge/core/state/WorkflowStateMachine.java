package com.flowforge.core.state;

import static com.flowforge.core.state.WorkflowStatus.CANCELLED;
import static com.flowforge.core.state.WorkflowStatus.CANCELLING;
import static com.flowforge.core.state.WorkflowStatus.COMPLETED;
import static com.flowforge.core.state.WorkflowStatus.CREATED;
import static com.flowforge.core.state.WorkflowStatus.FAILED;
import static com.flowforge.core.state.WorkflowStatus.PAUSED;
import static com.flowforge.core.state.WorkflowStatus.RUNNING;

/** The workflow execution lifecycle as one table. */
public final class WorkflowStateMachine {

    public static final StateMachine<WorkflowStatus> INSTANCE = StateMachine.builder("workflow execution", WorkflowStatus.class)
        .allow(CREATED, RUNNING)                                    // same transaction that creates the execution
        .allow(RUNNING, PAUSED, COMPLETED, FAILED, CANCELLING)
        .allow(PAUSED, RUNNING, COMPLETED, FAILED, CANCELLING)      // in-flight tasks may still finish while paused
        .allow(CANCELLING, CANCELLED)                               // once no task is RUNNING
        .allow(FAILED, RUNNING, CANCELLED)                          // retry / replay, or give up
        .build();

    private WorkflowStateMachine() {
    }
}

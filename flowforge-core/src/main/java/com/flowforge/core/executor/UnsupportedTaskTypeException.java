package com.flowforge.core.executor;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;
import com.flowforge.core.workflow.TaskType;

/** No executor is registered for a task type; validation normally catches this before anything runs. */
public class UnsupportedTaskTypeException extends FlowForgeException {

    public UnsupportedTaskTypeException(TaskType type) {
        super(ErrorCode.INVALID_WORKFLOW_DEFINITION, "no executor is registered for task type " + type);
    }
}

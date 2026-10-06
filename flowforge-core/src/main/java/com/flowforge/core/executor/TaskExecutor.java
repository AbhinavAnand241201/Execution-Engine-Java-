package com.flowforge.core.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import java.util.List;

/**
 * The plug-in point for one kind of work. The engine never branches on task type; it asks the
 * {@link TaskExecutorRegistry} for the executor of a task's type, so adding a type means adding one class.
 */
public interface TaskExecutor {

    TaskType supportedType();

    /** Checks a task's config when the workflow is created or published; paths are relative to the config object. */
    List<ValidationError> validateConfig(JsonNode config);

    /**
     * Runs one attempt. Implementations should check {@link TaskContext#cancellation()} regularly and let
     * {@link InterruptedException} propagate, so timeouts and cancellation can stop them.
     */
    TaskResult execute(TaskContext context) throws InterruptedException;
}

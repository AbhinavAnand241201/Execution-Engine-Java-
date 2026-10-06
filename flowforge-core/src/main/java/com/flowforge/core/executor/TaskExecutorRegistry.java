package com.flowforge.core.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.workflow.ConfigValidator;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.ValidationErrorCode;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Finds the executor for a task type (Registry pattern), which is what lets the engine avoid
 * {@code if (type == HTTP)} branches. It is also the definition validator's config hook, so a workflow is only
 * accepted if every task's config makes sense to the executor that will run it.
 */
public final class TaskExecutorRegistry implements ConfigValidator {

    private final Map<TaskType, TaskExecutor> executors;

    /** Fails fast on two executors for one type: which one would run would otherwise depend on list order. */
    public TaskExecutorRegistry(List<? extends TaskExecutor> executors) {
        Map<TaskType, TaskExecutor> byType = new EnumMap<>(TaskType.class);
        for (TaskExecutor executor : executors) {
            TaskExecutor previous = byType.putIfAbsent(executor.supportedType(), executor);
            if (previous != null) {
                throw new IllegalArgumentException("two executors for task type " + executor.supportedType() + ": "
                    + previous.getClass().getSimpleName() + " and " + executor.getClass().getSimpleName());
            }
        }
        this.executors = Collections.unmodifiableMap(byType);
    }

    public TaskExecutor get(TaskType type) {
        TaskExecutor executor = executors.get(type);
        if (executor == null) {
            throw new UnsupportedTaskTypeException(type);
        }
        return executor;
    }

    public Set<TaskType> supportedTypes() {
        return executors.keySet();
    }

    @Override
    public List<ValidationError> validate(TaskType type, JsonNode config) {
        TaskExecutor executor = executors.get(type);
        if (executor == null) {
            return List.of(new ValidationError("", ValidationErrorCode.INVALID_TASK_CONFIG,
                "no executor is available for task type " + type));
        }
        return executor.validateConfig(config);
    }
}

package com.flowforge.core.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.testing.ScriptedExecutor;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.ValidationErrorCode;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.core.workflow.WorkflowDefinitionValidator;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TaskExecutorRegistryTest {

    @Test
    @DisplayName("T05.1 a duplicate executor type fails at construction")
    void duplicateTypeFails() {
        assertThatThrownBy(() -> new TaskExecutorRegistry(List.of(new DelayTaskExecutor(), ScriptedExecutor.succeeding())))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("DELAY");
    }

    @Test
    @DisplayName("T05.1 an unknown type throws UnsupportedTaskTypeException")
    void unknownTypeThrows() {
        TaskExecutorRegistry registry = new TaskExecutorRegistry(List.of(new DelayTaskExecutor()));

        assertThat(registry.get(TaskType.DELAY)).isInstanceOf(DelayTaskExecutor.class);
        assertThat(registry.supportedTypes()).containsExactly(TaskType.DELAY);
        assertThatThrownBy(() -> registry.get(TaskType.HTTP))
            .isInstanceOf(UnsupportedTaskTypeException.class)
            .hasMessageContaining("HTTP");
    }

    @Test
    @DisplayName("T05.extra-1 the registry is the validator's config hook: bad DELAY config and types without an executor are reported")
    void registryValidatesConfig() {
        TaskExecutorRegistry registry = new TaskExecutorRegistry(List.of(new DelayTaskExecutor()));
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("wait", TaskType.DELAY).config(JsonNodeFactory.instance.objectNode().put("durationMs", -5))
            .task("call", TaskType.HTTP)
            .build();

        List<ValidationError> errors = new WorkflowDefinitionValidator(registry, 200).validate(definition);

        assertThat(errors).extracting(ValidationError::path, ValidationError::code).containsExactlyInAnyOrder(
            tuple("tasks[0].config.durationMs", ValidationErrorCode.INVALID_TASK_CONFIG),
            tuple("tasks[1].config", ValidationErrorCode.INVALID_TASK_CONFIG));
    }
}

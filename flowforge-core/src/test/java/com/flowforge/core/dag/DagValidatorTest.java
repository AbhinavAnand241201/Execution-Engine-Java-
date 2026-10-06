package com.flowforge.core.dag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.ValidationErrorCode;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.core.workflow.WorkflowDefinitionValidator;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DagValidatorTest {

    private final DagValidator validator = new DagValidator(new WorkflowDefinitionValidator());

    @Test
    @DisplayName("T02.3 composed validator reports CYCLE_DETECTED with the cycle path in the message")
    void reportsCycleWithPath() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).dependsOn("c")
            .task("b", TaskType.DELAY).dependsOn("a")
            .task("c", TaskType.DELAY).dependsOn("b")
            .build();

        List<ValidationError> errors = validator.validate(definition);

        assertThat(errors).extracting(ValidationError::path, ValidationError::code)
            .containsExactly(tuple("tasks[0].dependsOn[0]", ValidationErrorCode.CYCLE_DETECTED));
        assertThat(errors.get(0).message()).contains("a → b → c → a");
    }

    @Test
    @DisplayName("T02.extra-5 structural errors and a cycle are reported together; a self dependency is not double-reported")
    void structuralErrorsAndCycleTogether() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).dependsOn("b", "ghost")
            .task("b", TaskType.DELAY).dependsOn("a")
            .task("s", TaskType.DELAY).dependsOn("s")
            .build();

        assertThat(validator.validate(definition)).extracting(ValidationError::path, ValidationError::code)
            .containsExactlyInAnyOrder(
                tuple("tasks[0].dependsOn[1]", ValidationErrorCode.UNKNOWN_DEPENDENCY),
                tuple("tasks[2].dependsOn[0]", ValidationErrorCode.SELF_DEPENDENCY),
                tuple("tasks[0].dependsOn[0]", ValidationErrorCode.CYCLE_DETECTED));
    }

    @Test
    @DisplayName("T02.extra-6 a valid definition has no errors")
    void validDefinition() {
        assertThat(validator.validate(WorkflowFixtures.diamond())).isEmpty();
    }
}

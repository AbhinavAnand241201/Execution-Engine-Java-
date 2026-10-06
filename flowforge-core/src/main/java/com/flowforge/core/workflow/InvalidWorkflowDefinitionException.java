package com.flowforge.core.workflow;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;
import java.util.List;

/**
 * Thrown when a definition can't be accepted; carries every problem found so the client can fix them in one
 * round trip. Maps to 422 {@code INVALID_WORKFLOW_DEFINITION} with the errors as {@code details}.
 */
public class InvalidWorkflowDefinitionException extends FlowForgeException {

    private final List<ValidationError> errors;

    public InvalidWorkflowDefinitionException(List<ValidationError> errors) {
        super(ErrorCode.INVALID_WORKFLOW_DEFINITION, "Workflow definition is invalid (" + errors.size() + " error(s))");
        this.errors = List.copyOf(errors);
    }

    public List<ValidationError> errors() {
        return errors;
    }
}

package com.flowforge.core.workflow;

/**
 * Why one field of a workflow definition is invalid. These appear in the {@code details} of a 422
 * {@code INVALID_WORKFLOW_DEFINITION} response, so clients can react per field instead of parsing messages.
 */
public enum ValidationErrorCode {
    MALFORMED_JSON,
    UNKNOWN_FIELD,
    DUPLICATE_FIELD,
    INVALID_VALUE,
    REQUIRED,
    TOO_FEW_TASKS,
    TOO_MANY_TASKS,
    INVALID_TASK_KEY,
    DUPLICATE_TASK_KEY,
    UNKNOWN_DEPENDENCY,
    SELF_DEPENDENCY,
    DUPLICATE_DEPENDENCY,
    OUT_OF_RANGE,
    CYCLE_DETECTED,
    INVALID_TASK_CONFIG
}

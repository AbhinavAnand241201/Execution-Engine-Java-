package com.flowforge.runtime.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.ValidationErrorCode;
import java.util.List;
import java.util.Set;

/** Small helpers so every executor reports config problems the same way (path relative to {@code config}). */
final class ConfigChecks {

    private ConfigChecks() {
    }

    static ValidationError invalid(String path, String message) {
        return new ValidationError(path, ValidationErrorCode.INVALID_TASK_CONFIG, message);
    }

    static void rejectUnknownFields(JsonNode config, Set<String> known, List<ValidationError> errors) {
        config.fieldNames().forEachRemaining(field -> {
            if (!known.contains(field)) {
                errors.add(invalid(field, "unknown field '" + field + "'"));
            }
        });
    }

    /** The text of a required non-blank string field, or null after recording the error. */
    static String requiredText(JsonNode config, String field, List<ValidationError> errors) {
        JsonNode value = config.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            errors.add(invalid(field, field + " is required and must be a non-empty string"));
            return null;
        }
        return value.asText();
    }
}

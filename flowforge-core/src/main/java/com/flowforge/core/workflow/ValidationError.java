package com.flowforge.core.workflow;

import java.util.Objects;

/**
 * One problem in a workflow definition, located by a JSON path such as {@code tasks[2].dependsOn[0]} so a client
 * can highlight the exact field. Validators return lists of these instead of throwing, which is what makes
 * collect-all validation possible.
 *
 * @param path JSON path of the offending field; {@code ""} for the document itself
 */
public record ValidationError(String path, ValidationErrorCode code, String message) {

    public ValidationError {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(code, "code");
        Objects.requireNonNull(message, "message");
    }

    /** The same error located under {@code prefix}, e.g. {@code durationMs} → {@code tasks[1].config.durationMs}. */
    public ValidationError under(String prefix) {
        return new ValidationError(path.isEmpty() ? prefix : prefix + "." + path, code, message);
    }
}

package com.flowforge.core.executor;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * What one attempt produced. Sealed, so whoever handles results (the local engine now, the orchestrator later)
 * must handle all three cases and the compiler flags a forgotten one.
 */
public sealed interface TaskResult {

    /** Finished; {@code output} becomes visible to dependents. */
    record Success(JsonNode output) implements TaskResult {
        public Success {
            output = output == null ? null : output.deepCopy();
        }
    }

    /** Didn't finish. {@code retryable} is the executor's opinion (e.g. HTTP 503 yes, 400 no). */
    record Failure(String errorCode, String message, boolean retryable) implements TaskResult {
    }

    /** Stopped because its {@link CancellationToken} was cancelled. */
    record Cancelled() implements TaskResult {
    }

    static TaskResult success(JsonNode output) {
        return new Success(output);
    }

    static TaskResult failure(String errorCode, String message, boolean retryable) {
        return new Failure(errorCode, message, retryable);
    }

    static TaskResult cancelled() {
        return new Cancelled();
    }
}

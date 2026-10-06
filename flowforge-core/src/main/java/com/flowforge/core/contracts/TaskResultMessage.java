package com.flowforge.core.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.state.AttemptOutcome;
import java.time.Instant;
import java.util.UUID;

/**
 * The outcome of one attempt, sent back to the orchestrator. The orchestrator applies it only if the task
 * is still RUNNING with the same attempt and worker, so a duplicate or a late result from a timed-out attempt is a
 * no-op.
 */
public record TaskResultMessage(UUID messageId, int schemaVersion, UUID taskExecutionId, UUID workflowExecutionId,
                                int attempt, String workerId, AttemptOutcome outcome, JsonNode output,
                                String errorCode, String errorMessage, boolean retryable,
                                Instant startedAt, Instant finishedAt, String correlationId) {

    public static final int SCHEMA_VERSION = 1;

    public TaskResultMessage {
        output = output == null ? null : output.deepCopy();
    }
}

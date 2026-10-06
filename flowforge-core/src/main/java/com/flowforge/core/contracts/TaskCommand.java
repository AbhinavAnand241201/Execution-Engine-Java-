package com.flowforge.core.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.workflow.TaskType;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * "Run attempt N of this task". Deliberately small: upstream outputs are not included; the worker loads
 * them from PostgreSQL after it has claimed the attempt (claim-check). {@code attempt} is the fencing token: only a
 * command whose attempt matches the row can claim it, so a stale or duplicate command can never run the task.
 */
public record TaskCommand(UUID messageId, int schemaVersion, UUID taskExecutionId, UUID workflowExecutionId,
                          String taskKey, TaskType type, int attempt, JsonNode config, List<String> dependsOn,
                          long timeoutMs, String correlationId, Instant issuedAt) {

    public static final int SCHEMA_VERSION = 1;

    public TaskCommand {
        config = config == null ? null : config.deepCopy();
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
    }
}

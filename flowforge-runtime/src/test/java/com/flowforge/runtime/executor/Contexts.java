package com.flowforge.runtime.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.executor.CancellationToken;
import com.flowforge.core.executor.TaskContext;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/** Builds task contexts for executor tests. */
final class Contexts {

    static final ObjectMapper JSON = new ObjectMapper();

    private Contexts() {
    }

    static JsonNode json(String text) {
        try {
            return JSON.readTree(text);
        } catch (Exception e) {
            throw new IllegalArgumentException(text, e);
        }
    }

    static TaskContext context(JsonNode config) {
        return context(UUID.randomUUID(), 1, config, Map.of(), null, new CancellationToken());
    }

    static TaskContext context(UUID taskExecutionId, int attempt, JsonNode config, Map<String, JsonNode> upstream,
                               Instant deadline, CancellationToken token) {
        return new TaskContext(UUID.randomUUID(), taskExecutionId, "task", attempt, config, upstream, deadline, token,
            "corr-" + taskExecutionId);
    }
}

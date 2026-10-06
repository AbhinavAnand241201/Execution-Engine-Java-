package com.flowforge.core.executor;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Everything one attempt needs, handed to the executor so it never has to look anything up itself.
 *
 * @param upstreamOutputs outputs of the task's dependencies by key, plus the execution input as {@code "$input"}
 * @param deadline        when the engine will time the attempt out; long operations can pass it on (e.g. HTTP timeout)
 */
public record TaskContext(UUID workflowExecutionId, UUID taskExecutionId, String taskKey, int attempt, JsonNode config,
                          Map<String, JsonNode> upstreamOutputs, Instant deadline, CancellationToken cancellation,
                          String correlationId) {

    /** The execution input is exposed to every task under this key. */
    public static final String INPUT_KEY = "$input";

    /** Copies the JSON so one executor can't change what another task (or a retry) sees. */
    public TaskContext {
        config = config == null ? null : config.deepCopy();
        Map<String, JsonNode> outputs = new LinkedHashMap<>();
        if (upstreamOutputs != null) {
            upstreamOutputs.forEach((key, value) -> outputs.put(key, value.deepCopy()));
        }
        upstreamOutputs = Map.copyOf(outputs);
    }
}

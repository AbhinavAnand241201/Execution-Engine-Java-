package com.flowforge.core.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

/**
 * Validates a task's {@code config} for its type. It is a hook, not part of the definition validator, because only
 * the executor for a type knows what its config means; the executor registry implements it.
 */
@FunctionalInterface
public interface ConfigValidator {

    /** Accepts any config; used until executors are registered. */
    ConfigValidator ACCEPT_ALL = (type, config) -> List.of();

    /** Returns errors with paths relative to the config object (e.g. {@code durationMs}), or an empty list. */
    List<ValidationError> validate(TaskType type, JsonNode config);
}

package com.flowforge.core.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.time.Duration;
import java.util.List;

/**
 * One node of the DAG as the user defined it. Immutable all the way down, because a published version is shared
 * by every execution of it (and cached); a change would silently alter runs already in progress.
 *
 * <p>{@code dependsOn} is an ordered list, not a set: validation errors point at {@code dependsOn[j]}, and a
 * duplicate entry must survive long enough to be reported.
 *
 * <p>A missing or {@code null} config becomes {@code {}}; a config that is not an object is kept so the validator
 * can report it.
 *
 * <p>{@code key} and {@code type} may be {@code null} here so the validator can report them as {@code REQUIRED}
 * alongside every other error, instead of the constructor failing on the first one.
 */
public record TaskDefinition(String key, TaskType type, JsonNode config, List<String> dependsOn, Duration timeout,
                             Priority priority, RetryPolicySpec retry) {

    public static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);
    public static final Priority DEFAULT_PRIORITY = Priority.NORMAL;

    /** Applies defaults and takes defensive copies, so no caller-held object can change this task later. */
    public TaskDefinition {
        boolean absent = config == null || config.isNull() || config.isMissingNode();
        config = absent ? JsonNodeFactory.instance.objectNode() : config.deepCopy();
        dependsOn = dependsOn == null ? List.of() : List.copyOf(dependsOn);
        timeout = timeout == null ? DEFAULT_TIMEOUT : timeout;
        priority = priority == null ? DEFAULT_PRIORITY : priority;
        retry = retry == null ? RetryPolicySpec.DEFAULT : retry;
    }

    /**
     * Returns a deep copy. Jackson has no read-only {@code JsonNode}, so returning the field would let any caller
     * mutate a shared, published definition. Configs are small, so copying on read is cheap enough.
     */
    @Override
    public JsonNode config() {
        return config.deepCopy();
    }
}

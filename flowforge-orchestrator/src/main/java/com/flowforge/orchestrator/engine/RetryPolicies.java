package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.retry.RetryPolicy;
import com.flowforge.core.workflow.BackoffType;
import com.flowforge.core.workflow.RetryPolicySpec;
import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Turns the retry policy snapshot on a task row ({@code task_executions.retry_policy}, the canonical JSON of the
 * published definition with every default written out) into the core {@link RetryPolicy}.
 */
final class RetryPolicies {

    private final ObjectMapper json;
    private final RandomGenerator random;

    RetryPolicies(ObjectMapper json, RandomGenerator random) {
        this.json = json;
        this.random = random;
    }

    RetryPolicy fromSnapshot(String snapshot) {
        JsonNode node;
        try {
            node = json.readTree(snapshot);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored retry policy is not JSON", e);
        }
        RetryPolicySpec spec = new RetryPolicySpec(
            node.path("maxAttempts").asInt(RetryPolicySpec.DEFAULT_MAX_ATTEMPTS),
            node.hasNonNull("backoff") ? BackoffType.valueOf(node.get("backoff").asText()) : null,
            node.hasNonNull("initialDelayMs") ? Duration.ofMillis(node.get("initialDelayMs").asLong()) : null,
            node.hasNonNull("maxDelayMs") ? Duration.ofMillis(node.get("maxDelayMs").asLong()) : null,
            node.path("multiplier").asDouble(RetryPolicySpec.DEFAULT_MULTIPLIER),
            node.path("jitter").asBoolean(RetryPolicySpec.DEFAULT_JITTER));
        return RetryPolicy.from(spec, random);
    }
}

package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** What the execution endpoints return. */
public final class ExecutionViews {

    private ExecutionViews() {
    }

    public record ExecutionSummary(UUID id, UUID workflowId, UUID workflowVersionId, int version, String status,
                                   String correlationId, Instant createdAt, Instant startedAt, Instant completedAt) {
    }

    public record ExecutionDetail(UUID id, UUID workflowId, UUID workflowVersionId, int version, String status,
                                  JsonNode input, String correlationId, Instant createdAt, Instant startedAt,
                                  Instant completedAt, List<TaskView> tasks) {
    }

    public record TaskView(UUID id, String key, String type, String status, int priority, int attempt, int retryCount,
                           int maxAttempts, long timeoutMs, String workerId, Instant readyAt, Instant queuedAt,
                           Instant startedAt, Instant nextAttemptAt, Instant completedAt, JsonNode output,
                           String errorCode, String errorMessage) {
    }

    public record EventView(long sequence, UUID eventId, String type, UUID taskExecutionId, JsonNode data,
                            Instant occurredAt) {
    }

    public record AttemptView(int attempt, String workerId, String outcome, Instant startedAt, Instant finishedAt,
                              String errorCode, String errorMessage) {
    }

    /** {@code replayed} = an earlier request with the same Idempotency-Key created this execution. */
    public record ExecuteResult(ExecutionDetail execution, boolean replayed) {
    }
}

package com.flowforge.core.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

/**
 * Something that happened to an execution or a worker, published on {@code ff.workflow.events} /
 * {@code ff.worker.events}. Delivery is at-least-once (transactional outbox), so consumers deduplicate by
 * {@code eventId}, which is also the id of the matching row in the execution's timeline.
 */
public record DomainEvent(UUID eventId, String type, UUID workflowExecutionId, UUID taskExecutionId, String workerId,
                          Instant occurredAt, JsonNode data) {

    public DomainEvent {
        data = data == null ? null : data.deepCopy();
    }
}

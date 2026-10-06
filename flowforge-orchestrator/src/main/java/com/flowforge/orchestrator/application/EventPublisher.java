package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.DomainEvent;
import com.flowforge.core.contracts.Topics;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxWriter;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxWriter.OutboxMessage;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The one way execution events are emitted: each event is written to the execution's timeline
 * ({@code workflow_events}, what the API shows) and to the outbox for {@code ff.workflow.events}, in the caller's
 * transaction, with the same event id. So the timeline and the published stream can't disagree: both exist only if
 * the state change committed.
 */
public class EventPublisher {

    private final ExecutionJdbcRepository executions;
    private final OutboxWriter outbox;
    private final ObjectMapper json;

    public EventPublisher(ExecutionJdbcRepository executions, OutboxWriter outbox, ObjectMapper json) {
        this.executions = executions;
        this.outbox = outbox;
        this.json = json;
    }

    /** Must run inside the transaction that made the change (the outbox write is {@code MANDATORY}). */
    public void publish(UUID executionId, String correlationId, List<NewEvent> events, Instant now) {
        if (events.isEmpty()) {
            return;
        }
        List<UUID> eventIds = new ArrayList<>();
        for (NewEvent event : events) {
            UUID eventId = UUID.randomUUID();
            eventIds.add(eventId);
            JsonNode data = read(event.data());
            String workerId = data != null && data.hasNonNull("workerId") ? data.get("workerId").asText() : null;
            DomainEvent domainEvent = new DomainEvent(eventId, event.type(), executionId, event.taskExecutionId(),
                workerId, now, data);
            outbox.write(new OutboxMessage(eventId, Topics.WORKFLOW_EVENTS, executionId.toString(), event.type(),
                write(domainEvent), Map.of(Topics.HEADER_CORRELATION_ID, correlationId)), now);
        }
        executions.insertEvents(executionId, eventIds, events, now);
    }

    private JsonNode read(String text) {
        try {
            return text == null ? null : json.readTree(text);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("event data is not JSON", e);
        }
    }

    private String write(DomainEvent event) {
        try {
            return json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize event " + event.type(), e);
        }
    }

    /** A worker event (registered, draining, offline) on {@code ff.worker.events}: outbox only, no execution timeline. */
    public void publishWorkerEvent(String workerId, String type, Instant now) {
        UUID eventId = UUID.randomUUID();
        outbox.write(new OutboxMessage(eventId, Topics.WORKER_EVENTS, workerId, type,
            write(new DomainEvent(eventId, type, null, null, workerId, now, null)), Map.of()), now);
    }
}

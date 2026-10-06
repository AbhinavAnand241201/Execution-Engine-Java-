package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.state.WorkflowStateMachine;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.LockedExecution;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Instant;
import java.util.List;

/** Final-state moves of a locked execution (held by the caller), shared by result processing and the jobs. */
final class WorkflowTransitions {

    private WorkflowTransitions() {
    }

    static void finish(EngineJdbcRepository engine, ObjectMapper json, LockedExecution execution, WorkflowStatus to,
                       String eventType, Instant now, List<NewEvent> events) {
        WorkflowStatus from = WorkflowStatus.valueOf(execution.status());
        if (!WorkflowStateMachine.INSTANCE.canTransition(from, to)) {
            return;   // e.g. already FAILED, or CANCELLING: cancellation decides the final state
        }
        if (engine.setExecutionStatus(execution.id(), from.name(), to.name(), now, true) == 1) {
            events.add(new NewEvent(null, eventType, json.createObjectNode().put("status", to.name()).toString()));
        }
    }

    /** A CANCELLING execution becomes CANCELLED once nothing of it is queued or running any more (used by cancellation). */
    static void completeCancellationIfIdle(EngineJdbcRepository engine, ObjectMapper json, LockedExecution execution,
                                           Instant now, List<NewEvent> events) {
        if (!WorkflowStatus.CANCELLING.name().equals(execution.status())) {
            return;
        }
        boolean busy = engine.taskStates(execution.id()).values().stream()
            .anyMatch(t -> t.status().equals("QUEUED") || t.status().equals("RUNNING"));
        if (!busy && engine.setExecutionStatus(execution.id(), "CANCELLING", "CANCELLED", now, true) == 1) {
            events.add(new NewEvent(null, "WorkflowCancelled", json.createObjectNode().put("status", "CANCELLED")
                .toString()));
        }
    }
}

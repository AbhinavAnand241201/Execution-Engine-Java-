package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.Candidate;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.CommandData;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Moves READY tasks to QUEUED and sends their commands (DispatchJob). Candidates come from a plain SELECT in
 * priority order (CRITICAL first, then oldest READY), so priority works even though Kafka has none: higher-priority
 * tasks simply get dispatched first. Each candidate is then handled in its own transaction: lock the execution,
 * re-check, conditional READY → QUEUED with {@code attempt + 1}, and hand the command to the dispatcher, which
 * delivers only after commit.
 */
public class DispatchService {

    private final EngineJdbcRepository engine;
    private final EventPublisher publisher;
    private final TaskDispatcher dispatcher;
    private final CapacityProvider capacity;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final int batchSize;

    public DispatchService(EngineJdbcRepository engine, EventPublisher publisher, TaskDispatcher dispatcher,
                           CapacityProvider capacity, TransactionTemplate transaction, Clock clock, ObjectMapper json,
                           int batchSize) {
        this.engine = engine;
        this.publisher = publisher;
        this.dispatcher = dispatcher;
        this.capacity = capacity;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.batchSize = batchSize;
    }

    /** One pass; returns how many tasks were dispatched. */
    public int dispatchOnce() {
        List<Candidate> candidates = engine.readyCandidates(batchSize);
        if (candidates.isEmpty()) {
            return 0;
        }
        CapacityProvider.Budget budget = capacity.budget();
        int dispatched = 0;
        for (Candidate candidate : candidates) {
            if (!budget.tryTake(candidate.type())) {
                continue;   // no room for this type; a lower-priority task of another type may still fit
            }
            if (Boolean.TRUE.equals(transaction.execute(status -> dispatch(candidate)))) {
                dispatched++;
            } else {
                budget.giveBack(candidate.type());   // not dispatched after all (paused, or someone else took it)
            }
        }
        return dispatched;
    }

    private boolean dispatch(Candidate candidate) {
        var execution = engine.lockExecution(candidate.workflowExecutionId()).orElse(null);
        if (execution == null || !"RUNNING".equals(execution.status())) {
            return false;   // paused, cancelling or finished since the SELECT
        }
        Instant now = clock.instant();
        TaskStateMachine.INSTANCE.transition(candidate.id(), TaskStatus.READY, TaskStatus.QUEUED);
        Integer attempt = engine.queueTask(candidate.id(), now).orElse(null);
        if (attempt == null) {
            return false;   // someone else dispatched it, or it was cancelled
        }
        CommandData data = engine.commandData(candidate.id());
        publisher.publish(data.workflowExecutionId(), data.correlationId(), List.of(new NewEvent(data.taskExecutionId(),
            "TaskQueued", json.createObjectNode().put("taskKey", data.taskKey()).put("attempt", attempt).toString())),
            now);
        dispatcher.dispatch(command(data, attempt, now));
        return true;
    }

    /**
     * Sends the command of a task that has been QUEUED for too long again, for the same attempt with a new message
     * id (QueueReconciler): the first command was lost (crash before delivery, failed delivery). If it
     * wasn't lost after all, the claim's attempt check lets only one of the two run. Returns true if re-sent.
     */
    public boolean resend(UUID taskId, UUID executionId) {
        return Boolean.TRUE.equals(transaction.execute(status -> {
            var execution = engine.lockExecution(executionId).orElse(null);
            var task = engine.findTask(taskId).orElse(null);
            if (execution == null || !"RUNNING".equals(execution.status()) || task == null
                || !TaskStatus.QUEUED.name().equals(task.status())) {
                return false;
            }
            Instant now = clock.instant();
            if (engine.touchQueued(taskId, task.attempt(), now) != 1) {
                return false;
            }
            dispatcher.dispatch(command(engine.commandData(taskId), task.attempt(), now));
            return true;
        }));
    }

    private TaskCommand command(CommandData data, int attempt, Instant now) {
        return new TaskCommand(UUID.randomUUID(), TaskCommand.SCHEMA_VERSION, data.taskExecutionId(),
            data.workflowExecutionId(), data.taskKey(), data.type(), attempt, readConfig(data.config()), data.dependsOn(),
            data.timeoutMs(), data.correlationId(), now);
    }

    private com.fasterxml.jackson.databind.JsonNode readConfig(String config) {
        try {
            return json.readTree(config);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored task config is not JSON", e);
        }
    }
}

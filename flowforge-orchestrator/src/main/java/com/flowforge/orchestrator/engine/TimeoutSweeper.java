package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.engine.FailureHandler.AttemptFailure;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.JobCandidate;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.TaskSnapshot;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * RUNNING tasks past {@code deadline_at + grace} → attempt TIMED_OUT → the normal failure path. The worker
 * enforces the timeout itself; this is the orchestrator's backstop for when the worker can't report (hung, crashed,
 * partitioned). The grace keeps it from racing a worker that is just about to report its own timeout. A result that
 * arrives later for the timed-out attempt is stale: the task is no longer RUNNING on that attempt.
 */
public class TimeoutSweeper {

    private final EngineJdbcRepository engine;
    private final EventPublisher publisher;
    private final FailureHandler failures;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final Duration grace;
    private final int batchSize;

    public TimeoutSweeper(EngineJdbcRepository engine, EventPublisher publisher, FailureHandler failures,
                          TransactionTemplate transaction, Clock clock, ObjectMapper json,
                          Duration grace, int batchSize) {
        this.engine = engine;
        this.publisher = publisher;
        this.failures = failures;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.grace = grace;
        this.batchSize = batchSize;
    }

    /** One pass; returns how many attempts this call timed out. */
    public int sweep() {
        int timedOut = 0;
        for (JobCandidate candidate : engine.overdueRunning(clock.instant().minus(grace), batchSize)) {
            if (Boolean.TRUE.equals(transaction.execute(status -> timeOut(candidate)))) {
                timedOut++;
            }
        }
        return timedOut;
    }

    private boolean timeOut(JobCandidate candidate) {
        var execution = engine.lockExecution(candidate.workflowExecutionId()).orElse(null);
        TaskSnapshot task = engine.findTask(candidate.taskId()).orElse(null);
        Instant now = clock.instant();
        // Re-check under the lock: the result may have been applied since the SELECT.
        if (execution == null || task == null || !TaskStatus.RUNNING.name().equals(task.status())
            || task.deadlineAt() == null || !task.deadlineAt().plus(grace).isBefore(now)) {
            return false;
        }
        List<NewEvent> events = new ArrayList<>();
        AttemptFailure failure = new AttemptFailure(AttemptOutcome.TIMED_OUT, AttemptOutcome.TIMED_OUT.name(),
            "no result " + grace.toMillis() + " ms after the deadline " + task.deadlineAt(), true, task.startedAt(), now);
        if (!failures.handle(execution, task, failure, now, events)) {
            return false;
        }
        WorkflowTransitions.completeCancellationIfIdle(engine, json, execution, now, events);
        publisher.publish(execution.id(), execution.correlationId(), events, now);
        return true;
    }
}

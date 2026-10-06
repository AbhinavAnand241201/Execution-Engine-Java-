package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.retry.FailureInfo;
import com.flowforge.core.retry.RetryDecision;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.orchestrator.infrastructure.persistence.DeadLetterJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.LockedExecution;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.TaskSnapshot;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Instant;
import java.util.List;
import java.util.random.RandomGenerator;

/**
 * The one failure path for a RUNNING attempt, shared by result processing, the timeout sweeper and worker
 * recovery, so a failure is handled the same way however it was detected. The caller holds the execution lock
 * and has checked the task is RUNNING on this attempt.
 *
 * <p>The task's own retry policy decides: retry → RETRYING with {@code next_attempt_at} stored in the row (the
 * {@link RetryPromoter} makes it READY when due, so a waiting retry holds no thread and survives restarts); give up →
 * FAILED_PERMANENTLY, a dead-letter entry, and the execution FAILED. In a CANCELLING execution nothing is retried:
 * the task becomes CANCELLED.
 */
public class FailureHandler {

    /** Why an attempt didn't succeed, as reported by a worker or observed by the orchestrator. */
    public record AttemptFailure(AttemptOutcome outcome, String errorCode, String errorMessage,
                                 boolean executorSaysRetryable, Instant startedAt, Instant finishedAt) {
    }

    private final EngineJdbcRepository engine;
    private final DeadLetterJdbcRepository deadLetters;
    private final RetryPolicies policies;
    private final ObjectMapper json;

    public FailureHandler(EngineJdbcRepository engine, DeadLetterJdbcRepository deadLetters, ObjectMapper json,
                          RandomGenerator random) {
        this.engine = engine;
        this.deadLetters = deadLetters;
        this.policies = new RetryPolicies(json, random);
        this.json = json;
    }

    /**
     * Applies the failure; false if the task had already left RUNNING for this attempt (nothing changed). Events are
     * appended to {@code events}; the caller writes them in the same transaction.
     */
    public boolean handle(LockedExecution execution, TaskSnapshot task, AttemptFailure failure, Instant now,
                          List<NewEvent> events) {
        if (WorkflowStatus.CANCELLING.name().equals(execution.status())) {
            return cancel(task, failure, now, events);
        }
        String failedEvent = failure.outcome() == AttemptOutcome.TIMED_OUT ? "TaskTimedOut" : "TaskAttemptFailed";
        RetryDecision decision = policies.fromSnapshot(task.retryPolicy()).decide(task.retryCount(),
            new FailureInfo(failure.outcome(), failure.errorCode(), failure.executorSaysRetryable()));
        return switch (decision) {
            case RetryDecision.Retry retry -> {
                TaskStateMachine.INSTANCE.transition(task.id(), TaskStatus.RUNNING, TaskStatus.RETRYING);
                Instant due = now.plus(retry.delay());
                if (engine.scheduleRetry(task.id(), task.attempt(), task.workerId(), due, failure.errorCode(),
                    failure.errorMessage(), now) != 1) {
                    yield false;
                }
                recordAttempt(task, failure);
                events.add(event(task, failedEvent, failure.errorCode()));
                events.add(new NewEvent(task.id(), "TaskRetryScheduled", data(task)
                    .put("retryCount", task.retryCount() + 1).put("delayMs", retry.delay().toMillis())
                    .put("nextAttemptAt", due.toString()).toString()));
                yield true;
            }
            case RetryDecision.GiveUp giveUp -> {
                TaskStateMachine.INSTANCE.transition(task.id(), TaskStatus.RUNNING, TaskStatus.FAILED_PERMANENTLY);
                if (engine.finishTask(task.id(), task.attempt(), task.workerId(), TaskStatus.FAILED_PERMANENTLY.name(),
                    null, failure.errorCode(), failure.errorMessage(), now).isEmpty()) {
                    yield false;
                }
                recordAttempt(task, failure);
                deadLetters.insert(task.id(), execution.id(), task.attempt(), failure.errorCode(), failure.errorMessage(),
                    now);
                events.add(event(task, failedEvent, failure.errorCode()));
                events.add(new NewEvent(task.id(), "TaskDeadLettered", data(task).put("reason", giveUp.reason())
                    .toString()));
                WorkflowTransitions.finish(engine, json, execution, WorkflowStatus.FAILED, "WorkflowFailed", now, events);
                yield true;
            }
        };
    }

    private boolean cancel(TaskSnapshot task, AttemptFailure failure, Instant now, List<NewEvent> events) {
        TaskStateMachine.INSTANCE.transition(task.id(), TaskStatus.RUNNING, TaskStatus.CANCELLED);
        if (engine.finishTask(task.id(), task.attempt(), task.workerId(), TaskStatus.CANCELLED.name(), null,
            failure.errorCode(), failure.errorMessage(), now).isEmpty()) {
            return false;
        }
        recordAttempt(task, failure);
        events.add(event(task, "TaskCancelled", failure.errorCode()));
        return true;
    }

    private void recordAttempt(TaskSnapshot task, AttemptFailure failure) {
        engine.insertAttempt(task.id(), task.attempt(), task.workerId(), failure.outcome().name(), failure.startedAt(),
            failure.finishedAt(), failure.errorCode(), failure.errorMessage());
    }

    private NewEvent event(TaskSnapshot task, String type, String errorCode) {
        ObjectNode data = data(task);
        if (errorCode != null) {
            data.put("errorCode", errorCode);
        }
        return new NewEvent(task.id(), type, data.toString());
    }

    private ObjectNode data(TaskSnapshot task) {
        return json.createObjectNode().put("taskKey", task.taskKey()).put("attempt", task.attempt())
            .put("workerId", task.workerId());
    }
}

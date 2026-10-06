package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.dag.ReadinessCalculator;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.application.PublishedDagCache;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.engine.FailureHandler.AttemptFailure;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.LockedExecution;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.TaskSnapshot;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.TaskState;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Applies one attempt's result. Everything happens in one transaction that starts by locking the execution
 * row, which is what makes the join race safe: when B, C and D finish together, their transactions run one after
 * another, and the last one sees all three SUCCEEDED and makes E READY exactly once.
 *
 * <p>The task update is conditional on RUNNING + attempt + worker: a duplicate result, or a late result from an
 * attempt that already timed out, updates nothing and is dropped. Failures go to the {@link FailureHandler} (retry or
 * dead-letter).
 */
public class ResultProcessor {

    private static final Logger log = LoggerFactory.getLogger(ResultProcessor.class);
    /**
     * States in which a success makes dependents READY. FAILED is included (they become READY but are not
     * dispatched, since dispatch needs RUNNING), so a later replay of the failed task can't leave them PENDING forever.
     */
    private static final Set<WorkflowStatus> PROMOTES_DEPENDENTS =
        Set.of(WorkflowStatus.RUNNING, WorkflowStatus.PAUSED, WorkflowStatus.FAILED);

    /** What happened to a result. */
    public enum Outcome { APPLIED, STALE }

    private final EngineJdbcRepository engine;
    private final EventPublisher publisher;
    private final PublishedDagCache dags;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final ReadyTaskSignal readyTasks;
    private final FailureHandler failures;
    private final Counter staleResults;
    private final Runnable beforeCommit;

    public ResultProcessor(EngineJdbcRepository engine, EventPublisher publisher, PublishedDagCache dags,
                           FailureHandler failures, TransactionTemplate transaction, Clock clock, ObjectMapper json,
                           ReadyTaskSignal readyTasks, MeterRegistry meters) {
        this(engine, publisher, dags, failures, transaction, clock, json, readyTasks, meters, () -> { });
    }

    /** {@code beforeCommit} runs after all writes, before the commit: tests inject a crash there (T17.6). */
    ResultProcessor(EngineJdbcRepository engine, EventPublisher publisher, PublishedDagCache dags,
                    FailureHandler failures, TransactionTemplate transaction, Clock clock, ObjectMapper json,
                    ReadyTaskSignal readyTasks, MeterRegistry meters, Runnable beforeCommit) {
        this.engine = engine;
        this.publisher = publisher;
        this.failures = failures;
        this.dags = dags;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.readyTasks = readyTasks;
        this.beforeCommit = beforeCommit;
        this.staleResults = Counter.builder("task.results.stale")
            .description("Results ignored because the attempt was no longer RUNNING (duplicate or zombie)")
            .register(meters);
    }

    public Outcome process(TaskResultMessage result) {
        return transaction.execute(status -> apply(result));
    }

    private Outcome apply(TaskResultMessage result) {
        LockedExecution execution = engine.lockExecution(result.workflowExecutionId()).orElse(null);
        TaskSnapshot task = engine.findTask(result.taskExecutionId()).orElse(null);
        if (execution == null || task == null || !task.workflowExecutionId().equals(execution.id())
            || !TaskStatus.RUNNING.name().equals(task.status()) || task.attempt() != result.attempt()
            || !Objects.equals(task.workerId(), result.workerId())) {
            return stale(result);   // duplicate, or a zombie attempt that already timed out / was superseded
        }
        Instant now = clock.instant();
        List<NewEvent> events = new ArrayList<>();
        boolean madeReady = false;
        switch (result.outcome()) {
            case SUCCEEDED -> {
                if (!finish(task, result, TaskStatus.SUCCEEDED, now)) {
                    return stale(result);
                }
                events.add(event(result, "TaskSucceeded", "taskKey", task.taskKey()));
                madeReady = onSuccess(execution, task.taskKey(), now, events);
            }
            // Every other outcome (FAILED, TIMED_OUT, CANCELLED, ...) is a failure. The handler cancels the task if the
            // execution is CANCELLING; otherwise it retries or dead-letters, so nothing is left RUNNING forever.
            default -> {
                AttemptFailure failure = new AttemptFailure(result.outcome(), result.errorCode(), result.errorMessage(),
                    result.retryable(), result.startedAt(), result.finishedAt());
                if (!failures.handle(execution, task, failure, now, events)) {
                    return stale(result);
                }
            }
        }
        WorkflowTransitions.completeCancellationIfIdle(engine, json, execution, now, events);
        publisher.publish(execution.id(), execution.correlationId(), events, now);
        beforeCommit.run();
        if (madeReady) {
            afterCommit(readyTasks::signal);
        }
        return Outcome.APPLIED;
    }

    /** RUNNING → {@code to} for this attempt, plus its attempt row; false if the row had already moved on. */
    private boolean finish(TaskSnapshot task, TaskResultMessage result, TaskStatus to, Instant now) {
        TaskStateMachine.INSTANCE.transition(task.id(), TaskStatus.RUNNING, to);
        String output = to == TaskStatus.SUCCEEDED && result.output() != null ? result.output().toString() : null;
        if (engine.finishTask(task.id(), result.attempt(), result.workerId(), to.name(), output, result.errorCode(),
            result.errorMessage(), now).isEmpty()) {
            return false;
        }
        engine.insertAttempt(task.id(), result.attempt(), result.workerId(), result.outcome().name(),
            result.startedAt(), result.finishedAt(), result.errorCode(), result.errorMessage());
        return true;
    }

    /** Makes newly satisfied dependents READY (unless cancelling) and completes the execution when all succeeded. */
    private boolean onSuccess(LockedExecution execution, String taskKey, Instant now, List<NewEvent> events) {
        WorkflowStatus workflow = WorkflowStatus.valueOf(execution.status());
        // Read after the lock: every result committed before ours is visible here.
        Map<String, TaskState> states = engine.taskStates(execution.id());
        Map<String, TaskStatus> statuses = new LinkedHashMap<>();
        states.forEach((key, state) -> statuses.put(key, TaskStatus.valueOf(state.status())));
        boolean madeReady = false;
        if (PROMOTES_DEPENDENTS.contains(workflow)) {
            ReadinessCalculator readiness = new ReadinessCalculator(dags.dag(execution.workflowVersionId()));
            for (String ready : readiness.newlyReady(taskKey, statuses)) {
                TaskState dependent = states.get(ready);
                TaskStateMachine.INSTANCE.transition(dependent.id(), TaskStatus.PENDING, TaskStatus.READY);
                if (engine.markReady(dependent.id(), now) == 1) {
                    events.add(new NewEvent(dependent.id(), "TaskReady", data("taskKey", ready)));
                    madeReady = true;
                }
            }
        }
        if (statuses.values().stream().allMatch(s -> s == TaskStatus.SUCCEEDED)) {
            WorkflowTransitions.finish(engine, json, execution, WorkflowStatus.COMPLETED, "WorkflowCompleted", now,
                events);
        }
        return madeReady;
    }

    private Outcome stale(TaskResultMessage result) {
        staleResults.increment();
        log.debug("stale result ignored taskExecutionId={} attempt={} worker={}", result.taskExecutionId(),
            result.attempt(), result.workerId());
        return Outcome.STALE;
    }

    private NewEvent event(TaskResultMessage result, String type, String... keyValues) {
        ObjectNode data = json.createObjectNode();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            data.put(keyValues[i], keyValues[i + 1]);
        }
        data.put("attempt", result.attempt());
        data.put("workerId", result.workerId());
        return new NewEvent(result.taskExecutionId(), type, data.toString());
    }

    private String data(String... keyValues) {
        ObjectNode data = json.createObjectNode();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            data.put(keyValues[i], keyValues[i + 1]);
        }
        return data.toString();
    }

    private static void afterCommit(Runnable action) {
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                action.run();
            }
        });
    }
}

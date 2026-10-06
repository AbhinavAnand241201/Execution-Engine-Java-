package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStateMachine;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.orchestrator.infrastructure.persistence.DeadLetterJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.DeadLetterJdbcRepository.DeadLetterRow;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.LockedExecution;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.TaskState;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.ExecutionRow;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The dead-letter queue and replays. A replay is an ordinary state change of the execution, so it runs
 * under the execution lock: dead task(s) FAILED_PERMANENTLY → READY with a fresh retry cycle, execution FAILED →
 * RUNNING, DLQ entries marked replayed. Replaying is idempotent: the second replay of an entry finds it already
 * replayed (409), and a concurrent one waits on the lock and then sees the same.
 */
public class DeadLetterService {

    /** One DLQ entry as the API shows it. */
    public record DeadLetterView(UUID id, UUID taskExecutionId, UUID workflowExecutionId, UUID workflowId, String taskKey,
                                 int attempt, String errorCode, String errorMessage, Instant createdAt,
                                 Instant replayedAt, UUID replayedBy) {
    }

    /** What a replay did. */
    public record ReplayView(UUID workflowExecutionId, String status, List<String> replayedTasks) {
    }

    private final DeadLetterJdbcRepository deadLetters;
    private final EngineJdbcRepository engine;
    private final ExecutionJdbcRepository executions;
    private final EventPublisher publisher;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final ReadyTaskSignal readyTasks;

    public DeadLetterService(DeadLetterJdbcRepository deadLetters, EngineJdbcRepository engine,
                             ExecutionJdbcRepository executions, EventPublisher publisher, TransactionTemplate transaction, Clock clock,
                             ObjectMapper json, ReadyTaskSignal readyTasks) {
        this.deadLetters = deadLetters;
        this.engine = engine;
        this.executions = executions;
        this.publisher = publisher;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.readyTasks = readyTasks;
    }

    public Page<DeadLetterView> list(UUID workflowId, String errorCode, boolean includeReplayed, Pageable pageable) {
        boolean newestFirst = pageable.getSort().stream().findFirst().map(o -> o.isDescending()).orElse(true);
        List<DeadLetterView> items = deadLetters.list(workflowId, errorCode, includeReplayed, newestFirst,
            pageable.getPageSize(), pageable.getOffset()).stream().map(DeadLetterService::view).toList();
        return new PageImpl<>(items, pageable, deadLetters.count(workflowId, errorCode, includeReplayed));
    }

    /**
     * Replays one entry's task. Refused if the entry was already replayed, or if other tasks of the execution are dead
     * too (the execution would go back to RUNNING with them still failed: use the execution retry instead).
     */
    public ReplayView replay(AuthenticatedUser operator, UUID entryId) {
        DeadLetterRow entry = deadLetters.find(entryId).orElseThrow(() -> new NotFoundException("dead-letter entry",
            entryId));
        ReplayView result = transaction.execute(status -> {
            LockedExecution execution = lock(entry.workflowExecutionId());
            // Re-read under the lock: a concurrent replay may have committed while we waited.
            if (deadLetters.find(entryId).orElseThrow().replayedAt() != null) {
                throw new ApplicationException(ErrorCode.ALREADY_REPLAYED, "This dead-letter entry was already replayed");
            }
            requireFailed(execution);
            List<TaskState> dead = deadTasks(execution.id());
            if (dead.stream().anyMatch(t -> !t.id().equals(entry.taskExecutionId()))) {
                throw new ApplicationException(ErrorCode.OTHER_TASKS_FAILED, "Other tasks of this execution are also "
                    + "dead; retry the whole execution with POST /api/v1/executions/" + execution.id() + "/retry");
            }
            return reopen(execution, dead, entry.taskExecutionId(), operator);
        });
        readyTasks.signal();   // committed: the dispatcher can see the READY task
        return result;
    }

    /** Replays every dead task of one of the caller's executions ({@code POST /executions/{id}/retry}). */
    public ReplayView retryExecution(AuthenticatedUser caller, UUID executionId) {
        ExecutionRow row = executions.findExecution(executionId).orElse(null);
        if (row == null || (!row.ownerId().equals(caller.id()) && !caller.canReadAll())) {
            throw new NotFoundException("execution", executionId);
        }
        if (!row.ownerId().equals(caller.id())) {
            throw new ApplicationException(ErrorCode.FORBIDDEN, "Only the owner can retry an execution");
        }
        ReplayView result = transaction.execute(status -> {
            LockedExecution execution = lock(executionId);
            requireFailed(execution);
            return reopen(execution, deadTasks(executionId), null, caller);
        });
        readyTasks.signal();
        return result;
    }

    private LockedExecution lock(UUID executionId) {
        return engine.lockExecution(executionId).orElseThrow(() -> new NotFoundException("execution", executionId));
    }

    private static void requireFailed(LockedExecution execution) {
        WorkflowStatus status = WorkflowStatus.valueOf(execution.status());
        if (status != WorkflowStatus.FAILED) {   // only FAILED executions have dead tasks to replay
            throw new ApplicationException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                "Cannot retry an execution in state " + status);
        }
    }

    private List<TaskState> deadTasks(UUID executionId) {
        return engine.taskStates(executionId).values().stream()
            .filter(t -> TaskStatus.FAILED_PERMANENTLY.name().equals(t.status())).toList();
    }

    /** {@code onlyTask} null = every dead task. Caller holds the execution lock. */
    private ReplayView reopen(LockedExecution execution, List<TaskState> dead, UUID onlyTask, AuthenticatedUser by) {
        Instant now = clock.instant();
        List<String> replayed = new ArrayList<>();
        List<NewEvent> events = new ArrayList<>();
        for (TaskState task : dead) {
            if (onlyTask != null && !task.id().equals(onlyTask)) {
                continue;
            }
            TaskStateMachine.INSTANCE.transition(task.id(), TaskStatus.FAILED_PERMANENTLY, TaskStatus.READY);
            engine.replayTask(task.id(), now).ifPresent(key -> {
                replayed.add(key);
                events.add(new NewEvent(task.id(), "TaskReady", json.createObjectNode().put("taskKey", key)
                    .put("replay", true).toString()));
            });
        }
        if (replayed.isEmpty()) {
            throw new ApplicationException(ErrorCode.ALREADY_REPLAYED, "Nothing to replay: no dead task");
        }
        // Every dead task has an open entry (written in the same transaction that killed it), so 0 rows here means
        // the data is inconsistent: fail and roll the whole replay back rather than half-apply it.
        if (deadLetters.markReplayed(execution.id(), onlyTask, by.id(), now) < 1) {
            throw new IllegalStateException("no open dead-letter entry for execution " + execution.id());
        }
        WorkflowStateMachine.INSTANCE.transition(execution.id(), WorkflowStatus.FAILED, WorkflowStatus.RUNNING);
        if (engine.reopenFailedExecution(execution.id(), now) != 1) {
            throw new IllegalStateException("execution " + execution.id() + " left FAILED while locked");
        }
        ObjectNode data = json.createObjectNode().put("by", by.id().toString());
        ArrayNode tasks = data.putArray("tasks");
        replayed.forEach(tasks::add);
        events.add(new NewEvent(null, "WorkflowRetried", data.toString()));
        publisher.publish(execution.id(), execution.correlationId(), events, now);
        return new ReplayView(execution.id(), WorkflowStatus.RUNNING.name(), List.copyOf(replayed));
    }

    private static DeadLetterView view(DeadLetterRow row) {
        return new DeadLetterView(row.id(), row.taskExecutionId(), row.workflowExecutionId(), row.workflowId(),
            row.taskKey(), row.attempt(), row.errorCode(), row.errorMessage(), row.createdAt(), row.replayedAt(),
            row.replayedBy());
    }
}

package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStateMachine;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.core.workflow.Priority;
import com.flowforge.orchestrator.application.ExecutionViews.AttemptView;
import com.flowforge.orchestrator.application.ExecutionViews.EventView;
import com.flowforge.orchestrator.application.ExecutionViews.ExecuteResult;
import com.flowforge.orchestrator.application.ExecutionViews.ExecutionDetail;
import com.flowforge.orchestrator.application.ExecutionViews.ExecutionSummary;
import com.flowforge.orchestrator.application.ExecutionViews.TaskView;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.ExecutionRow;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewTask;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.TaskDefinitionRow;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowEntity;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowVersionEntity;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowVersionRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Starts executions and reads them back. Starting is one transaction: the execution, every task row (roots READY,
 * the rest PENDING) and the first events either all exist or none do. Retry policy and timeout are copied onto each
 * task row, so a run never depends on anything that could change after it started.
 */
public class ExecutionService {

    private final WorkflowRepository workflows;
    private final WorkflowVersionRepository versions;
    private final ExecutionJdbcRepository executions;
    private final EventPublisher publisher;
    private final PublishedDagCache dags;
    private final RequestHasher hasher;
    private final ObjectMapper json;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final int inputMaxBytes;
    private final ReadyTaskSignal readyTasks;

    public ExecutionService(WorkflowRepository workflows, WorkflowVersionRepository versions,
                            ExecutionJdbcRepository executions, EventPublisher publisher, PublishedDagCache dags, RequestHasher hasher,
                            ObjectMapper json, TransactionTemplate transaction, Clock clock, int inputMaxBytes,
                            ReadyTaskSignal readyTasks) {
        this.workflows = workflows;
        this.versions = versions;
        this.executions = executions;
        this.publisher = publisher;
        this.dags = dags;
        this.hasher = hasher;
        this.json = json;
        this.transaction = transaction;
        this.clock = clock;
        this.inputMaxBytes = inputMaxBytes;
        this.readyTasks = readyTasks;
    }

    /**
     * Idempotent with a key: the same key and the same request return the existing execution; the same key with a
     * different request is rejected. Concurrent duplicates race on the unique index {@code (owner_id, idempotency_key)};
     * the losers read and return the winner's execution.
     */
    public ExecuteResult execute(AuthenticatedUser caller, UUID workflowId, Integer version, JsonNode input,
                                 String idempotencyKey, String correlationId) {
        WorkflowEntity workflow = workflows.findByIdAndOwnerId(workflowId, caller.id())
            .orElseThrow(() -> new NotFoundException("workflow", workflowId));
        if (hasher.sizeInBytes(input) > inputMaxBytes) {
            throw new ApplicationException(ErrorCode.PAYLOAD_TOO_LARGE, "Execution input exceeds " + inputMaxBytes + " bytes");
        }
        ObjectNode request = json.createObjectNode();
        request.put("workflowId", workflowId.toString());
        request.set("version", version == null ? json.nullNode() : json.getNodeFactory().numberNode(version));
        request.set("input", input == null ? json.nullNode() : input);
        String requestHash = hasher.hash(request);

        if (idempotencyKey != null) {
            ExecutionRow existing = executions.findByIdempotencyKey(caller.id(), idempotencyKey).orElse(null);
            if (existing != null) {
                return replay(existing, requestHash);
            }
        }
        WorkflowVersionEntity published = resolveVersion(workflow, version);
        try {
            UUID id = transaction.execute(status -> create(caller, workflow, published, input, idempotencyKey, requestHash,
                correlationId));
            readyTasks.signal();   // committed: the root tasks are READY for the dispatcher now
            return new ExecuteResult(detail(executions.findExecution(id).orElseThrow()), false);
        } catch (DuplicateKeyException concurrentDuplicate) {
            // Every row inserted here has a fresh random id, so the only unique constraint that can fire is
            // ux_exec_idempotency: another request with this key committed first.
            if (idempotencyKey == null) {
                throw concurrentDuplicate;
            }
            return replay(executions.findByIdempotencyKey(caller.id(), idempotencyKey).orElseThrow(), requestHash);
        }
    }

    private ExecuteResult replay(ExecutionRow existing, String requestHash) {
        if (!existing.requestHash().equals(requestHash)) {
            throw new ApplicationException(ErrorCode.IDEMPOTENCY_KEY_REUSED,
                "This Idempotency-Key was already used with a different request");
        }
        return new ExecuteResult(detail(existing), true);
    }

    private WorkflowVersionEntity resolveVersion(WorkflowEntity workflow, Integer version) {
        Integer wanted = version != null ? version : workflow.getLatestPublishedVersion();
        if (wanted == null) {
            throw notPublished();
        }
        WorkflowVersionEntity entity = versions.findByWorkflowIdAndVersion(workflow.getId(), wanted)
            .orElseThrow(() -> new NotFoundException("workflow version " + wanted, workflow.getId()));
        if (!WorkflowVersionEntity.PUBLISHED.equals(entity.getStatus())) {
            throw notPublished();
        }
        return entity;
    }

    private UUID create(AuthenticatedUser caller, WorkflowEntity workflow, WorkflowVersionEntity version, JsonNode input,
                        String idempotencyKey, String requestHash, String correlationId) {
        Instant now = clock.instant();
        UUID executionId = UUID.randomUUID();
        WorkflowStatus status = WorkflowStateMachine.INSTANCE.transition(executionId, WorkflowStatus.CREATED,
            WorkflowStatus.RUNNING);
        executions.insertExecution(executionId, workflow.getId(), version.getId(), caller.id(), status.name(),
            input == null ? null : input.toString(), idempotencyKey, requestHash, correlationId, now);

        Set<String> roots = Set.copyOf(dags.dag(version.getId()).roots());
        List<NewTask> tasks = new ArrayList<>();
        List<NewEvent> events = new ArrayList<>();
        events.add(new NewEvent(null, "WorkflowStarted", eventData("workflowId", workflow.getId().toString(),
            "version", String.valueOf(version.getVersion()))));
        for (TaskDefinitionRow definition : executions.findTaskDefinitions(version.getId())) {
            UUID taskId = UUID.randomUUID();
            boolean root = roots.contains(definition.taskKey());
            TaskStatus taskStatus = root
                ? TaskStateMachine.INSTANCE.transition(taskId, TaskStatus.PENDING, TaskStatus.READY)
                : TaskStatus.PENDING;
            tasks.add(new NewTask(taskId, definition.id(), definition.taskKey(), definition.type(),
                Priority.valueOf(definition.priority()).weight(), taskStatus.name(), maxAttempts(definition.retryPolicy()),
                definition.retryPolicy(), definition.timeoutMs(), root ? now : null));
            if (root) {
                events.add(new NewEvent(taskId, "TaskReady", eventData("taskKey", definition.taskKey())));
            }
        }
        executions.insertTasks(executionId, tasks, now);
        publisher.publish(executionId, correlationId, events, now);
        return executionId;
    }

    public ExecutionDetail get(AuthenticatedUser caller, UUID executionId) {
        return detail(visible(caller, executionId));
    }

    public Page<ExecutionSummary> list(AuthenticatedUser caller, UUID workflowId, WorkflowStatus status, Instant createdFrom,
                                       Instant createdTo, Pageable pageable) {
        UUID owner = caller.canReadAll() ? null : caller.id();
        String statusName = status == null ? null : status.name();
        Sort.Order order = pageable.getSort().stream().findFirst().orElse(Sort.Order.desc("createdAt"));
        // Only constant column names reach the SQL; the client's sort text was whitelisted by PageRequestFactory.
        String orderBy = ("status".equals(order.getProperty()) ? "e.status" : "e.created_at")
            + (order.isAscending() ? " ASC" : " DESC") + ", e.id";
        List<ExecutionSummary> items = executions.listExecutions(owner, workflowId, statusName, createdFrom, createdTo,
            orderBy, pageable.getPageSize(), pageable.getOffset()).stream().map(ExecutionService::summary).toList();
        long total = executions.countExecutions(owner, workflowId, statusName, createdFrom, createdTo);
        return new PageImpl<>(items, pageable, total);
    }

    public Page<EventView> events(AuthenticatedUser caller, UUID executionId, Pageable pageable) {
        visible(caller, executionId);
        List<EventView> items = executions.findEvents(executionId, pageable.getPageSize(), pageable.getOffset()).stream()
            .map(e -> new EventView(e.id(), e.eventId(), e.type(), e.taskExecutionId(), read(e.data()), e.occurredAt()))
            .toList();
        return new PageImpl<>(items, pageable, executions.countEvents(executionId));
    }

    public List<AttemptView> attempts(AuthenticatedUser caller, UUID executionId, String taskKey) {
        visible(caller, executionId);
        if (!executions.taskExists(executionId, taskKey)) {
            throw new NotFoundException("task " + taskKey, executionId);
        }
        return executions.findAttempts(executionId, taskKey).stream()
            .map(a -> new AttemptView(a.attempt(), a.workerId(), a.outcome(), a.startedAt(), a.finishedAt(), a.errorCode(),
                a.errorMessage()))
            .toList();
    }

    /** Owners see their executions; OPERATOR and ADMIN see all; anyone else gets the same 404 as a missing id. */
    private ExecutionRow visible(AuthenticatedUser caller, UUID executionId) {
        ExecutionRow row = executions.findExecution(executionId).orElse(null);
        if (row == null || (!caller.canReadAll() && !row.ownerId().equals(caller.id()))) {
            throw new NotFoundException("execution", executionId);
        }
        return row;
    }

    private ExecutionDetail detail(ExecutionRow e) {
        List<TaskView> tasks = executions.findTasks(e.id()).stream()
            .map(t -> new TaskView(t.id(), t.taskKey(), t.type(), t.status(), t.priority(), t.attempt(), t.retryCount(),
                t.maxAttempts(), t.timeoutMs(), t.workerId(), t.readyAt(), t.queuedAt(), t.startedAt(), t.nextAttemptAt(),
                t.completedAt(), read(t.output()), t.errorCode(), t.errorMessage()))
            .toList();
        return new ExecutionDetail(e.id(), e.workflowId(), e.workflowVersionId(), e.version(), e.status(), read(e.input()),
            e.correlationId(), e.createdAt(), e.startedAt(), e.completedAt(), tasks);
    }

    private static ExecutionSummary summary(ExecutionRow e) {
        return new ExecutionSummary(e.id(), e.workflowId(), e.workflowVersionId(), e.version(), e.status(),
            e.correlationId(), e.createdAt(), e.startedAt(), e.completedAt());
    }

    private int maxAttempts(String retryPolicy) {
        JsonNode maxAttempts = read(retryPolicy).path("maxAttempts");
        if (!maxAttempts.canConvertToInt() || maxAttempts.asInt() < 1) {
            throw new IllegalStateException("published retry policy has no valid maxAttempts");
        }
        return maxAttempts.asInt();
    }

    private String eventData(String... keyValues) {
        ObjectNode data = json.createObjectNode();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            data.put(keyValues[i], keyValues[i + 1]);
        }
        return data.toString();
    }

    private JsonNode read(String text) {
        if (text == null) {
            return null;
        }
        try {
            return json.readTree(text);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored JSON is invalid", e);
        }
    }

    private static ApplicationException notPublished() {
        return new ApplicationException(ErrorCode.WORKFLOW_NOT_PUBLISHED, "The workflow has no published version to run");
    }
}

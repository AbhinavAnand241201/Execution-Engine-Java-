package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.JobCandidate;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * RETRYING tasks whose {@code next_attempt_at} has passed → READY. The retry timer is that column, not a
 * thread or an in-memory scheduler, so a restart loses nothing: the next pass promotes whatever is due. Candidates come
 * from a plain SELECT; each is promoted in its own transaction under the execution lock with a conditional
 * update, so two promoters running at once promote each task exactly once.
 */
public class RetryPromoter {

    /** A cancel in progress or finished: its RETRYING tasks are about to be (or were) cancelled, not retried. */
    private static final Set<String> SKIP = Set.of(WorkflowStatus.CANCELLING.name(), WorkflowStatus.CANCELLED.name(),
        WorkflowStatus.COMPLETED.name());

    private final EngineJdbcRepository engine;
    private final EventPublisher publisher;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ReadyTaskSignal readyTasks;
    private final ObjectMapper json;
    private final int batchSize;

    public RetryPromoter(EngineJdbcRepository engine, EventPublisher publisher, TransactionTemplate transaction,
                         Clock clock, ReadyTaskSignal readyTasks, ObjectMapper json, int batchSize) {
        this.engine = engine;
        this.publisher = publisher;
        this.transaction = transaction;
        this.clock = clock;
        this.readyTasks = readyTasks;
        this.json = json;
        this.batchSize = batchSize;
    }

    /** One pass; returns how many tasks this call promoted. */
    public int promoteDue() {
        int promoted = 0;
        for (JobCandidate candidate : engine.dueRetries(clock.instant(), batchSize)) {
            if (Boolean.TRUE.equals(transaction.execute(status -> promote(candidate)))) {
                promoted++;
            }
        }
        if (promoted > 0) {
            readyTasks.signal();   // after the commits above: the dispatcher can see the READY rows
        }
        return promoted;
    }

    private boolean promote(JobCandidate candidate) {
        var execution = engine.lockExecution(candidate.workflowExecutionId()).orElse(null);
        if (execution == null || SKIP.contains(execution.status())) {
            return false;
        }
        Instant now = clock.instant();
        TaskStateMachine.INSTANCE.transition(candidate.taskId(), TaskStatus.RETRYING, TaskStatus.READY);
        String taskKey = engine.promoteRetry(candidate.taskId(), now).orElse(null);
        if (taskKey == null) {
            return false;   // another promoter, or a cancel, got there first
        }
        publisher.publish(execution.id(), execution.correlationId(), List.of(new NewEvent(candidate.taskId(), "TaskReady",
            json.createObjectNode().put("taskKey", taskKey).put("retry", true).toString())), now);
        return true;
    }
}

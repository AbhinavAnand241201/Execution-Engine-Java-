package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.engine.FailureHandler.AttemptFailure;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.RunningAttempt;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.TaskSnapshot;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Recovers the RUNNING attempts of a worker that was declared OFFLINE. Each task is handled in its own
 * transaction under the execution lock, and only if it is still RUNNING on that worker with the same attempt:
 * a result that arrived meanwhile wins, and a second recovery of the same worker (two monitors) changes nothing.
 * The attempt ends WORKER_LOST and goes down the normal failure path, so it counts toward maxAttempts: a task that
 * kills every worker it runs on stops after maxAttempts instead of looping forever.
 *
 * <p>If the "dead" worker was only frozen (a zombie), its late result carries the old attempt number and is rejected
 * as stale; the retry runs with a higher attempt.
 */
public class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final EngineJdbcRepository engine;
    private final EventPublisher publisher;
    private final FailureHandler failures;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final ReadyTaskSignal readyTasks;

    public RecoveryService(EngineJdbcRepository engine, EventPublisher publisher, FailureHandler failures,
                           TransactionTemplate transaction, Clock clock, ObjectMapper json, ReadyTaskSignal readyTasks) {
        this.engine = engine;
        this.publisher = publisher;
        this.failures = failures;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.readyTasks = readyTasks;
    }

    /** Returns how many attempts this call recovered. One failing task doesn't stop the others. */
    public int recoverTasksOf(String workerId) {
        int recovered = 0;
        for (RunningAttempt candidate : engine.runningOn(workerId)) {
            try {
                if (Boolean.TRUE.equals(transaction.execute(status -> recover(candidate, workerId)))) {
                    recovered++;
                }
            } catch (RuntimeException e) {
                log.warn("recovery failed taskExecutionId={} workerId={} error={}", candidate.taskId(), workerId,
                    e.toString());
            }
        }
        if (recovered > 0) {
            readyTasks.signal();
        }
        return recovered;
    }

    private boolean recover(RunningAttempt candidate, String workerId) {
        var execution = engine.lockExecution(candidate.workflowExecutionId()).orElse(null);
        TaskSnapshot task = engine.findTask(candidate.taskId()).orElse(null);
        if (execution == null || task == null || !TaskStatus.RUNNING.name().equals(task.status())
            || !workerId.equals(task.workerId()) || task.attempt() != candidate.attempt()) {
            return false;   // finished, already recovered, or a newer attempt since the SELECT
        }
        Instant now = clock.instant();
        List<NewEvent> events = new ArrayList<>();
        AttemptFailure lost = new AttemptFailure(AttemptOutcome.WORKER_LOST, AttemptOutcome.WORKER_LOST.name(),
            "worker " + workerId + " went offline while running this attempt", true, task.startedAt(), now);
        if (!failures.handle(execution, task, lost, now, events)) {
            return false;
        }
        WorkflowTransitions.completeCancellationIfIdle(engine, json, execution, now, events);
        publisher.publish(execution.id(), execution.correlationId(), events, now);
        return true;
    }
}

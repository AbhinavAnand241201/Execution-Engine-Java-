package com.flowforge.orchestrator.engine;

import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository.JobCandidate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;

/**
 * Re-sends commands of tasks that stayed QUEUED for longer than {@code staleAfter} (QueueReconciler). A
 * command can be lost between the dispatch commit and its delivery (crash, failed delivery, runner shut down while it
 * waited for a permit); without this the task would sit in QUEUED forever and keep counting against capacity.
 */
public class QueueReconciler {

    private final EngineJdbcRepository engine;
    private final DispatchService dispatch;
    private final Clock clock;
    private final Duration staleAfter;
    private final int batchSize;
    private final Counter resent;

    public QueueReconciler(EngineJdbcRepository engine, DispatchService dispatch, Clock clock, Duration staleAfter,
                           int batchSize, MeterRegistry meters) {
        this.engine = engine;
        this.dispatch = dispatch;
        this.clock = clock;
        this.staleAfter = staleAfter;
        this.batchSize = batchSize;
        this.resent = Counter.builder("task.commands.resent").description("Commands re-sent for stale QUEUED tasks")
            .register(meters);
    }

    /** One pass; returns how many commands were re-sent. */
    public int reconcile() {
        int count = 0;
        for (JobCandidate candidate : engine.staleQueued(clock.instant().minus(staleAfter), batchSize)) {
            if (dispatch.resend(candidate.taskId(), candidate.workflowExecutionId())) {
                count++;
            }
        }
        resent.increment(count);
        return count;
    }
}

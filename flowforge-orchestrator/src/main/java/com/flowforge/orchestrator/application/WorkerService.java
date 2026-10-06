package com.flowforge.orchestrator.application;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository.WorkerRow;
import com.flowforge.orchestrator.infrastructure.redis.WorkerHeartbeats;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Worker registry for operators: list, inspect and drain workers. */
public class WorkerService {

    private static final Logger log = LoggerFactory.getLogger(WorkerService.class);

    /** How a worker looks through the API; {@code heartbeatAgeMs} = time since the monitor last saw a heartbeat. */
    public record WorkerView(String id, String name, String hostname, String status, List<String> capabilities,
                             int maxConcurrency, int currentLoad, Instant registeredAt, Instant lastSeenAt,
                             long heartbeatAgeMs, Instant offlineAt) {
    }

    private static final Duration DRAIN_FLAG_TTL = Duration.ofHours(24);

    private final WorkerJdbcRepository workers;
    private final WorkerHeartbeats heartbeats;
    private final EventPublisher publisher;
    private final TransactionTemplate transaction;
    private final Clock clock;

    public WorkerService(WorkerJdbcRepository workers, WorkerHeartbeats heartbeats, EventPublisher publisher,
                         TransactionTemplate transaction, Clock clock) {
        this.workers = workers;
        this.heartbeats = heartbeats;
        this.publisher = publisher;
        this.transaction = transaction;
        this.clock = clock;
    }

    public Page<WorkerView> list(String status, Pageable pageable) {
        List<WorkerView> items = workers.list(status, pageable.getPageSize(), pageable.getOffset()).stream()
            .map(this::view).toList();
        return new PageImpl<>(items, pageable, workers.count(status));
    }

    public WorkerView get(String id) {
        return view(workers.find(id).orElseThrow(() -> new ApplicationException(ErrorCode.NOT_FOUND,
            "worker " + id + " not found")));
    }

    /**
     * ACTIVE → DRAINING in the database (the record; capacity stops counting the worker at once), then, after the
     * commit, the Redis flag the worker reads on its next heartbeat to stop consuming. Draining a DRAINING worker is
     * a no-op; an OFFLINE one can't be drained.
     */
    public WorkerView drain(String id) {
        transaction.executeWithoutResult(status -> {
            WorkerRow worker = workers.find(id).orElseThrow(() -> new ApplicationException(ErrorCode.NOT_FOUND,
                "worker " + id + " not found"));
            if (workers.markDraining(id) == 1) {
                publisher.publishWorkerEvent(id, "WorkerDraining", clock.instant());
            } else if (!"DRAINING".equals(worker.status())) {
                throw new ApplicationException(ErrorCode.ILLEGAL_STATE_TRANSITION,
                    "Cannot drain a worker in state " + worker.status());
            }
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    try {
                        heartbeats.requestDrain(id, DRAIN_FLAG_TTL);
                    } catch (RuntimeException e) {
                        // The registry already says DRAINING (no new work is routed to it); the operator can repeat
                        // the request to deliver the flag once Redis is back. Not a reason to fail the request.
                        log.warn("drain flag not written workerId={} error={}", id, e.toString());
                    }
                }
            });
        });
        return get(id);
    }

    private WorkerView view(WorkerRow w) {
        long age = w.lastSeenAt() == null ? -1 : Duration.between(w.lastSeenAt(), clock.instant()).toMillis();
        return new WorkerView(w.id(), w.name(), w.hostname(), w.status(), w.capabilities(), w.maxConcurrency(),
            w.currentLoad(), w.registeredAt(), w.lastSeenAt(), age, w.offlineAt());
    }
}

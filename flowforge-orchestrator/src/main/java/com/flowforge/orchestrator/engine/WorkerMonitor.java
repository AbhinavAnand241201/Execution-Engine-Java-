package com.flowforge.orchestrator.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository.WorkerRow;
import com.flowforge.orchestrator.infrastructure.redis.WorkerHeartbeats;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Detects dead workers (WorkerMonitor). A live worker's heartbeat key exists; a worker whose key is gone, and
 * that has had a whole TTL to write it (since it registered and since Redis was last reachable), is marked OFFLINE
 * (conditional update: only one monitor wins), a WorkerOffline event is written, and its RUNNING attempts are
 * recovered.
 *
 * <p>If Redis itself doesn't answer, the pass is skipped: a missing key then says nothing about the worker, and
 * declaring every worker dead because Redis blinked would re-run every running task.
 */
public class WorkerMonitor {

    private static final Logger log = LoggerFactory.getLogger(WorkerMonitor.class);

    private final WorkerJdbcRepository workers;
    private final WorkerHeartbeats heartbeats;
    private final RecoveryService recovery;
    private final EventPublisher publisher;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final Duration heartbeatTtl;
    private final Counter skipped;
    private final EngineJdbcRepository engine;
    /** When Redis last became reachable (null while it isn't); only this monitor's thread touches it. */
    private Instant redisAvailableSince;

    public WorkerMonitor(WorkerJdbcRepository workers, EngineJdbcRepository engine, WorkerHeartbeats heartbeats,
                         RecoveryService recovery, EventPublisher publisher, TransactionTemplate transaction,
                         Clock clock, ObjectMapper json, Duration heartbeatTtl, MeterRegistry meters) {
        this.workers = workers;
        this.engine = engine;
        this.heartbeats = heartbeats;
        this.recovery = recovery;
        this.publisher = publisher;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.heartbeatTtl = heartbeatTtl;
        this.skipped = Counter.builder("monitor.skipped").description("Worker monitor passes skipped: Redis unavailable")
            .register(meters);
    }

    /** One pass; returns how many workers it declared OFFLINE. */
    public int monitorOnce() {
        Instant now = clock.instant();
        if (!heartbeats.available()) {
            redisAvailableSince = null;
            skipped.increment();
            log.warn("worker monitor skipped: Redis unavailable");
            return 0;
        }
        if (redisAvailableSince == null) {
            redisAvailableSince = now;   // back (or first pass): heartbeat keys may have expired meanwhile
        }
        List<WorkerRow> live = workers.live();
        Map<String, String> beats = heartbeats.heartbeats(live.stream().map(WorkerRow::id).toList()).orElse(null);
        if (beats == null) {
            skipped.increment();
            return 0;
        }
        int offline = 0;
        for (WorkerRow worker : live) {
            String beat = beats.get(worker.id());
            if (beat != null) {
                workers.touch(worker.id(), now, load(beat));
            } else if (silentForAWholeTtl(worker, now) && markOffline(worker.id(), now)) {
                offline++;
                int recovered = recovery.recoverTasksOf(worker.id());
                log.warn("worker offline workerId={} recoveredAttempts={}", worker.id(), recovered);
            }
        }
        // Attempts still RUNNING on workers that went OFFLINE a while ago: an earlier recovery was cut short
        // (orchestrator crash) or a graceful exit couldn't report them. Recovery is idempotent, so just run it.
        for (String workerId : engine.offlineWorkersWithRunningTasks(now.minus(heartbeatTtl))) {
            recovery.recoverTasksOf(workerId);
        }
        return offline;
    }

    /**
     * A missing key only proves a worker is dead once the worker had a whole TTL to write it: since it registered,
     * and since Redis came back (keys expire during an outage; workers need a beat to rewrite them).
     */
    private boolean silentForAWholeTtl(WorkerRow worker, Instant now) {
        Instant since = worker.registeredAt().isAfter(redisAvailableSince) ? worker.registeredAt() : redisAvailableSince;
        return since.isBefore(now.minus(heartbeatTtl));
    }

    private boolean markOffline(String workerId, Instant now) {
        return Boolean.TRUE.equals(transaction.execute(status -> {
            if (workers.markOffline(workerId, now) != 1) {
                return false;   // another monitor, or the worker's own graceful shutdown, got there first
            }
            publisher.publishWorkerEvent(workerId, "WorkerOffline", now);
            return true;
        }));
    }

    private int load(String heartbeat) {
        try {
            return json.readTree(heartbeat).path("load").asInt(0);
        } catch (JsonProcessingException e) {
            return 0;
        }
    }
}

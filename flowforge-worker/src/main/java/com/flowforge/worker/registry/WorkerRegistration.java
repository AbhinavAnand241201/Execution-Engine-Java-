package com.flowforge.worker.registry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.DomainEvent;
import com.flowforge.core.contracts.Topics;
import com.flowforge.core.workflow.TaskType;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The worker's own row in {@code workers}. Each change also writes a worker event to the outbox in the same
 * transaction (the orchestrator's relay publishes it on {@code ff.worker.events}), so the registry and the event
 * stream agree. PostgreSQL is the record; the Redis heartbeat only says "still alive".
 */
public class WorkerRegistration {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final ObjectMapper json;

    public WorkerRegistration(JdbcTemplate jdbc, TransactionTemplate transaction, ObjectMapper json) {
        this.jdbc = jdbc;
        this.transaction = transaction;
        this.json = json;
    }

    /**
     * A new incarnation: one row, ACTIVE straight away (it registers just before it starts consuming). Timestamps
     * come from the database clock, which the orchestrator's monitor also compares against.
     */
    public void register(String workerId, String name, String hostname, Set<TaskType> capabilities, int maxConcurrency,
                         Instant now) {
        transaction.executeWithoutResult(status -> {
            jdbc.update("""
                    INSERT INTO workers (id, name, hostname, status, capabilities, max_concurrency, current_load,
                                         registered_at, last_seen_at)
                    VALUES (?, ?, ?, 'ACTIVE', ?, ?, 0, now(), now())
                    """, workerId, name, hostname, capabilities.stream().map(Enum::name).sorted().toArray(String[]::new),
                maxConcurrency);
            event(workerId, "WorkerRegistered", now);
        });
    }

    /** This incarnation's status in the registry (null if the row is missing). */
    public String status(String workerId) {
        return jdbc.query("SELECT status FROM workers WHERE id = ?", (rs, n) -> rs.getString(1), workerId).stream()
            .findFirst().orElse(null);
    }

    /** ACTIVE/DRAINING → OFFLINE (graceful shutdown). False if the monitor already did it. */
    public boolean markOffline(String workerId, Instant now) {
        Boolean changed = transaction.execute(status -> {
            int rows = jdbc.update("UPDATE workers SET status = 'OFFLINE', offline_at = ?, current_load = 0 "
                + "WHERE id = ? AND status IN ('ACTIVE', 'DRAINING')", Timestamp.from(now), workerId);
            if (rows == 1) {
                event(workerId, "WorkerOffline", now);
            }
            return rows == 1;
        });
        return Boolean.TRUE.equals(changed);
    }

    private void event(String workerId, String type, Instant now) {
        UUID eventId = UUID.randomUUID();
        DomainEvent event = new DomainEvent(eventId, type, null, null, workerId, now, null);
        String payload;
        try {
            payload = json.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize worker event", e);
        }
        jdbc.update("INSERT INTO outbox (message_id, topic, message_key, message_type, payload, headers, created_at) "
                + "VALUES (?, ?, ?, ?, ?::jsonb, '{}'::jsonb, ?)", eventId, Topics.WORKER_EVENTS, workerId, type, payload,
            Timestamp.from(now));
    }
}

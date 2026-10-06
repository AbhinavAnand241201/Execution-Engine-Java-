package com.flowforge.orchestrator.infrastructure.persistence;

import com.flowforge.core.workflow.TaskType;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * The worker registry ({@code workers}). Status changes are conditional ({@code WHERE status IN ...}) and
 * return the row count, so two monitors (or a monitor and the worker's own shutdown) can't both mark a worker
 * OFFLINE and both run recovery.
 */
@Repository
public class WorkerJdbcRepository {

    private final JdbcTemplate jdbc;

    public WorkerJdbcRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public record WorkerRow(String id, String name, String hostname, String status, List<String> capabilities,
                            int maxConcurrency, int currentLoad, Instant registeredAt, Instant lastSeenAt,
                            Instant offlineAt) {
    }

    private static final String COLUMNS = "id, name, hostname, status, capabilities, max_concurrency, current_load, "
        + "registered_at, last_seen_at, offline_at";

    public List<WorkerRow> live() {
        return jdbc.query("SELECT " + COLUMNS + " FROM workers WHERE status IN ('ACTIVE', 'DRAINING') ORDER BY id",
            WorkerJdbcRepository::row);
    }

    public Optional<WorkerRow> find(String id) {
        return jdbc.query("SELECT " + COLUMNS + " FROM workers WHERE id = ?", WorkerJdbcRepository::row, id).stream()
            .findFirst();
    }

    /** Newest first; {@code status} null = all. */
    public List<WorkerRow> list(String status, int limit, long offset) {
        return jdbc.query("SELECT " + COLUMNS + " FROM workers WHERE (?::text IS NULL OR status = ?) "
            + "ORDER BY registered_at DESC, id LIMIT ? OFFSET ?", WorkerJdbcRepository::row, status, status, limit, offset);
    }

    public long count(String status) {
        Long count = jdbc.queryForObject("SELECT count(*) FROM workers WHERE (?::text IS NULL OR status = ?)",
            Long.class, status, status);
        return count == null ? 0 : count;
    }

    public void touch(String id, Instant seenAt, int load) {
        jdbc.update("UPDATE workers SET last_seen_at = ?, current_load = ? WHERE id = ? AND status IN ('ACTIVE', "
            + "'DRAINING')", Timestamp.from(seenAt), load, id);
    }

    /** ACTIVE/DRAINING → OFFLINE; 1 only for the caller that actually made the change. */
    public int markOffline(String id, Instant now) {
        return jdbc.update("UPDATE workers SET status = 'OFFLINE', offline_at = ?, current_load = 0 "
            + "WHERE id = ? AND status IN ('ACTIVE', 'DRAINING')", Timestamp.from(now), id);
    }

    /** ACTIVE → DRAINING. */
    public int markDraining(String id) {
        return jdbc.update("UPDATE workers SET status = 'DRAINING' WHERE id = ? AND status = 'ACTIVE'", id);
    }

    /** Σ max_concurrency of ACTIVE workers per capability: DRAINING and OFFLINE workers take nothing. */
    public Map<TaskType, Integer> activeCapacityByType() {
        Map<TaskType, Integer> capacity = new EnumMap<>(TaskType.class);
        jdbc.query("SELECT c.capability, sum(w.max_concurrency) FROM workers w, unnest(w.capabilities) AS c(capability) "
            + "WHERE w.status = 'ACTIVE' GROUP BY c.capability", (ResultSet rs) -> {
                try {
                    capacity.put(TaskType.valueOf(rs.getString(1)), rs.getInt(2));
                } catch (IllegalArgumentException unknownToThisVersion) {
                    // A newer worker with a type this orchestrator doesn't know (rolling upgrade): not capacity here.
                }
            });
        return capacity;
    }

    private static WorkerRow row(ResultSet rs, int n) throws SQLException {
        Array capabilities = rs.getArray("capabilities");
        List<String> list = capabilities == null ? List.of() : new ArrayList<>(List.of((String[]) capabilities.getArray()));
        return new WorkerRow(rs.getString("id"), rs.getString("name"), rs.getString("hostname"), rs.getString("status"),
            List.copyOf(list), rs.getInt("max_concurrency"), rs.getInt("current_load"), instant(rs, "registered_at"),
            instant(rs, "last_seen_at"), instant(rs, "offline_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }
}

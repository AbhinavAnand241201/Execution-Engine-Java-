package com.flowforge.orchestrator.engine;

import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.workflow.TaskType;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * {@code queue.depth{status,type}} gauges (QueueDepthSampler): how many tasks wait in READY, QUEUED,
 * RUNNING and RETRYING per type. Refreshed by a job every 10 s with one GROUP BY, so scraping metrics never queries
 * the database. A rising READY depth for a type means no capacity for it (no live worker, or all busy).
 */
public class QueueDepthSampler {

    private static final TaskStatus[] SAMPLED = {TaskStatus.READY, TaskStatus.QUEUED, TaskStatus.RUNNING,
        TaskStatus.RETRYING};

    private final JdbcTemplate jdbc;
    private final Map<String, AtomicLong> depths = new ConcurrentHashMap<>();

    public QueueDepthSampler(JdbcTemplate jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        for (TaskStatus status : SAMPLED) {
            for (TaskType type : TaskType.values()) {
                AtomicLong depth = new AtomicLong();
                depths.put(key(status.name(), type.name()), depth);
                Gauge.builder("queue.depth", depth, AtomicLong::get).tag("status", status.name())
                    .tag("type", type.name()).description("Tasks per status and type").register(meters);
            }
        }
    }

    public void sample() {
        Map<String, Long> fresh = new java.util.HashMap<>();
        jdbc.query("SELECT status, type, count(*) FROM task_executions WHERE status IN ('READY', 'QUEUED', 'RUNNING', "
            + "'RETRYING') GROUP BY status, type", (java.sql.ResultSet rs) -> {
                fresh.put(key(rs.getString(1), rs.getString(2)), rs.getLong(3));
            });
        depths.forEach((key, depth) -> depth.set(fresh.getOrDefault(key, 0L)));
    }

    private static String key(String status, String type) {
        return status + "/" + type;
    }
}

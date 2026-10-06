package com.flowforge.orchestrator.infrastructure.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Writes a published definition as rows ({@code task_definitions}, {@code task_dependencies}). The version keeps its
 * canonical JSON (every default written out; JSONB orders keys its own way); the rows exist so execution creation
 * and reporting can use plain SQL joins instead of digging through JSON.
 */
@Repository
public class TaskDefinitionWriter {

    private final JdbcTemplate jdbc;

    public TaskDefinitionWriter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** @param normalizedDefinition the definition with every default filled in (parser's canonical JSON) */
    public void insert(UUID workflowVersionId, JsonNode normalizedDefinition) {
        List<Object[]> tasks = new ArrayList<>();
        List<Object[]> dependencies = new ArrayList<>();
        for (JsonNode task : normalizedDefinition.get("tasks")) {
            String key = task.get("key").asText();
            tasks.add(new Object[] {UUID.randomUUID(), workflowVersionId, key, task.get("type").asText(),
                task.get("config").toString(), task.get("timeoutMs").asLong(), task.get("priority").asText(),
                task.get("retry").toString()});
            for (JsonNode dependency : task.get("dependsOn")) {
                dependencies.add(new Object[] {workflowVersionId, key, dependency.asText()});
            }
        }
        jdbc.batchUpdate("INSERT INTO task_definitions (id, workflow_version_id, task_key, type, config, timeout_ms, "
            + "priority, retry_policy) VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?::jsonb)", tasks);
        jdbc.batchUpdate("INSERT INTO task_dependencies (workflow_version_id, task_key, depends_on_task_key) "
            + "VALUES (?, ?, ?)", dependencies);
    }
}

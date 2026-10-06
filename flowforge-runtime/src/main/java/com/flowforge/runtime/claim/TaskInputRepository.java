package com.flowforge.runtime.claim;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.executor.TaskContext;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Loads what a claimed task needs (claim-check pattern): the execution input under {@code "$input"} and the outputs of
 * its {@code dependsOn} tasks by key. Outputs of SUCCEEDED tasks never change, so reading them after the claim is
 * consistent without any lock.
 */
public class TaskInputRepository {

    private final DataSource dataSource;
    private final ObjectMapper json;

    public TaskInputRepository(DataSource dataSource, ObjectMapper json) {
        this.dataSource = dataSource;
        this.json = json;
    }

    public Map<String, JsonNode> load(TaskCommand command) {
        Map<String, JsonNode> inputs = new LinkedHashMap<>();
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement select = connection.prepareStatement(
                "SELECT input::text FROM workflow_executions WHERE id = ?")) {
                select.setObject(1, command.workflowExecutionId());
                try (ResultSet rows = select.executeQuery()) {
                    if (rows.next() && rows.getString(1) != null) {
                        inputs.put(TaskContext.INPUT_KEY, json.readTree(rows.getString(1)));
                    }
                }
            }
            if (!command.dependsOn().isEmpty()) {
                try (PreparedStatement select = connection.prepareStatement("""
                    SELECT task_key, output::text FROM task_executions
                    WHERE workflow_execution_id = ? AND task_key = ANY (?) AND status = 'SUCCEEDED'
                    """)) {
                    Array keys = connection.createArrayOf("varchar", command.dependsOn().toArray());
                    select.setObject(1, command.workflowExecutionId());
                    select.setArray(2, keys);
                    try (ResultSet rows = select.executeQuery()) {
                        while (rows.next()) {
                            String output = rows.getString(2);
                            inputs.put(rows.getString(1), output == null ? json.nullNode() : json.readTree(output));
                        }
                    }
                }
            }
        } catch (SQLException | JsonProcessingException e) {
            throw new IllegalStateException("cannot load inputs for task " + command.taskExecutionId(), e);
        }
        return inputs;
    }
}

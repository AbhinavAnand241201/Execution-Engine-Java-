package com.flowforge.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An immutable picture of one execution: the final result when returned by {@code completion()}, or the latest
 * state when returned by {@code snapshot()}. Maps keep definition order.
 */
public record ExecutionReport(UUID executionId, WorkflowStatus status, Map<String, TaskStatus> tasks,
                              Map<String, Integer> attempts, Map<String, JsonNode> outputs, List<TimelineEvent> timeline) {

    /** Timeline events of one task and type, in order. */
    public List<TimelineEvent> events(String taskKey, TimelineEvent.Type type) {
        return timeline.stream().filter(e -> taskKey.equals(e.taskKey()) && e.type() == type).toList();
    }

    /** The retry delays chosen for a task, in order. */
    public List<Duration> retryDelays(String taskKey) {
        return events(taskKey, TimelineEvent.Type.RETRY_SCHEDULED).stream().map(e -> Duration.parse(e.detail())).toList();
    }
}

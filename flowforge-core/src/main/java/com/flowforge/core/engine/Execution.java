package com.flowforge.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.dag.Dag;
import com.flowforge.core.dag.ReadinessCalculator;
import com.flowforge.core.retry.RetryPolicy;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.random.RandomGenerator;

/**
 * The mutable state of one execution. Only the coordinator thread reads or writes the fields; other threads see
 * only the two volatile fields: the {@link #snapshot} it publishes and the {@link #acceptingClaims} flag.
 */
final class Execution {

    final UUID id = UUID.randomUUID();
    final Dag dag;
    final ReadinessCalculator readiness;
    final JsonNode input;
    final Map<String, TaskDefinition> definitions = new LinkedHashMap<>();
    final Map<String, UUID> taskIds = new HashMap<>();
    final Map<String, RetryPolicy> retryPolicies = new HashMap<>();
    final CompletableFuture<ExecutionReport> completion = new CompletableFuture<>();

    WorkflowStatus status = WorkflowStatus.CREATED;
    final Map<String, TaskStatus> statuses = new LinkedHashMap<>();
    final Map<String, Integer> attempts = new LinkedHashMap<>();
    final Map<String, Integer> retryCounts = new HashMap<>();
    final Map<String, JsonNode> outputs = new LinkedHashMap<>();
    final Map<String, AttemptControl> active = new HashMap<>();
    /** The runnable of each QUEUED task, so a task rejected while paused can be handed out again on resume. */
    final Map<String, PrioritizedTask> queued = new HashMap<>();
    /** QUEUED tasks whose claim was rejected because the execution wasn't RUNNING. */
    final Set<String> parked = new HashSet<>();
    /** Timed-out attempts ("key#attempt") whose thread hasn't reported back yet. */
    final Set<String> zombies = new HashSet<>();
    final List<TimelineEvent> timeline = new ArrayList<>();

    volatile ExecutionReport snapshot;
    /** Written by the coordinator, read by workers before they start an attempt: true only while RUNNING. */
    volatile boolean acceptingClaims;

    Execution(WorkflowDefinition definition, JsonNode input, RandomGenerator random) {
        this.dag = Dag.of(definition);
        this.readiness = new ReadinessCalculator(dag);
        this.input = input == null ? null : input.deepCopy();
        for (TaskDefinition task : definition.tasks()) {
            definitions.put(task.key(), task);
            taskIds.put(task.key(), UUID.randomUUID());
            retryPolicies.put(task.key(), RetryPolicy.from(task.retry(), random));
            statuses.put(task.key(), TaskStatus.PENDING);
            attempts.put(task.key(), 0);
            retryCounts.put(task.key(), 0);
        }
        publish();
    }

    void record(Instant at, String taskKey, int attempt, TimelineEvent.Type type, String detail) {
        timeline.add(new TimelineEvent(at, taskKey, attempt, type, detail));
    }

    boolean any(TaskStatus... wanted) {
        for (TaskStatus status : statuses.values()) {
            for (TaskStatus w : wanted) {
                if (status == w) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Copies the current state into an immutable report for other threads. */
    void publish() {
        snapshot = new ExecutionReport(id, status,
            Collections.unmodifiableMap(new LinkedHashMap<>(statuses)),
            Collections.unmodifiableMap(new LinkedHashMap<>(attempts)),
            Collections.unmodifiableMap(new LinkedHashMap<>(outputs)),
            List.copyOf(timeline));
    }
}

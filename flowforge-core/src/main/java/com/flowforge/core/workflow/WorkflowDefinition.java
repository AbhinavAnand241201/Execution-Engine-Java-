package com.flowforge.core.workflow;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * A workflow as the user wrote it: a list of tasks whose {@code dependsOn} edges form a DAG. This is the unit that
 * gets validated, versioned and published. Structural validity is not enforced here; see
 * {@link WorkflowDefinitionValidator}.
 */
public record WorkflowDefinition(List<TaskDefinition> tasks) {

    public WorkflowDefinition {
        tasks = tasks == null ? List.of() : List.copyOf(tasks);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Readable construction in tests and code: {@code builder().task("a", DELAY).task("b", HTTP).dependsOn("a")}.
     * Methods after {@code task(...)} configure that most recently added task.
     */
    public static final class Builder {

        private final List<TaskDefinition> tasks = new ArrayList<>();

        private Builder() {
        }

        public Builder task(String key, TaskType type) {
            tasks.add(new TaskDefinition(key, type, null, null, null, null, null));
            return this;
        }

        public Builder dependsOn(String... keys) {
            TaskDefinition t = current();
            return replaceCurrent(new TaskDefinition(t.key(), t.type(), t.config(), List.of(keys), t.timeout(), t.priority(),
                t.retry()));
        }

        public Builder config(JsonNode config) {
            TaskDefinition t = current();
            return replaceCurrent(new TaskDefinition(t.key(), t.type(), config, t.dependsOn(), t.timeout(), t.priority(),
                t.retry()));
        }

        public Builder timeout(Duration timeout) {
            TaskDefinition t = current();
            return replaceCurrent(new TaskDefinition(t.key(), t.type(), t.config(), t.dependsOn(), timeout, t.priority(),
                t.retry()));
        }

        public Builder priority(Priority priority) {
            TaskDefinition t = current();
            return replaceCurrent(new TaskDefinition(t.key(), t.type(), t.config(), t.dependsOn(), t.timeout(), priority,
                t.retry()));
        }

        public Builder retry(RetryPolicySpec retry) {
            TaskDefinition t = current();
            return replaceCurrent(new TaskDefinition(t.key(), t.type(), t.config(), t.dependsOn(), t.timeout(), t.priority(),
                retry));
        }

        public WorkflowDefinition build() {
            return new WorkflowDefinition(tasks);
        }

        private TaskDefinition current() {
            if (tasks.isEmpty()) {
                throw new IllegalStateException("call task(key, type) first");
            }
            return tasks.get(tasks.size() - 1);
        }

        private Builder replaceCurrent(TaskDefinition task) {
            tasks.set(tasks.size() - 1, task);
            return this;
        }
    }
}

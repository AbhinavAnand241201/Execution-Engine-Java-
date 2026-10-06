package com.flowforge.core.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class WorkflowDefinitionTest {

    @Test
    @DisplayName("T01.7 defaults: NORMAL priority, 60 s timeout, EXPONENTIAL/3/1000/60000/2.0/jitter, empty config and deps")
    void defaultsApplied() {
        TaskDefinition task = new TaskDefinition("a", TaskType.DELAY, null, null, null, null, null);

        assertThat(task.priority()).isEqualTo(Priority.NORMAL);
        assertThat(task.timeout()).isEqualTo(Duration.ofSeconds(60));
        assertThat(task.dependsOn()).isEmpty();
        assertThat(task.config().isObject()).isTrue();
        assertThat(task.config().isEmpty()).isTrue();

        RetryPolicySpec retry = task.retry();
        assertThat(retry.maxAttempts()).isEqualTo(3);
        assertThat(retry.backoff()).isEqualTo(BackoffType.EXPONENTIAL);
        assertThat(retry.initialDelay()).isEqualTo(Duration.ofMillis(1_000));
        assertThat(retry.maxDelay()).isEqualTo(Duration.ofMillis(60_000));
        assertThat(retry.multiplier()).isEqualTo(2.0);
        assertThat(retry.jitter()).isTrue();
        assertThat(retry).isEqualTo(RetryPolicySpec.DEFAULT);
    }

    @Test
    @DisplayName("T01.7 priority weights are CRITICAL 40, HIGH 30, NORMAL 20, LOW 10")
    void priorityWeights() {
        assertThat(Priority.CRITICAL.weight()).isEqualTo(40);
        assertThat(Priority.HIGH.weight()).isEqualTo(30);
        assertThat(Priority.NORMAL.weight()).isEqualTo(20);
        assertThat(Priority.LOW.weight()).isEqualTo(10);
    }

    @Test
    @DisplayName("T01.8 mutating the source list, dependency list or JsonNode after construction changes nothing")
    void sourcesAreCopied() {
        ObjectNode config = JsonNodeFactory.instance.objectNode().put("durationMs", 10);
        List<String> dependsOn = new ArrayList<>(List.of("a"));
        TaskDefinition b = new TaskDefinition("b", TaskType.DELAY, config, dependsOn, null, null, null);
        List<TaskDefinition> tasks = new ArrayList<>(List.of(
            new TaskDefinition("a", TaskType.DELAY, null, null, null, null, null), b));
        WorkflowDefinition definition = new WorkflowDefinition(tasks);

        config.put("durationMs", 999);
        dependsOn.add("zzz");
        tasks.clear();

        assertThat(definition.tasks()).hasSize(2);
        assertThat(definition.tasks().get(1).dependsOn()).containsExactly("a");
        assertThat(definition.tasks().get(1).config().get("durationMs").asInt()).isEqualTo(10);
    }

    @Test
    @DisplayName("T01.8 returned collections throw on mutation and returned config is a copy")
    void accessorsDoNotLeakMutableState() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).config(JsonNodeFactory.instance.objectNode().put("durationMs", 10))
            .task("b", TaskType.DELAY).dependsOn("a")
            .build();

        assertThatThrownBy(() -> definition.tasks().add(definition.tasks().get(0)))
            .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> definition.tasks().get(1).dependsOn().add("x"))
            .isInstanceOf(UnsupportedOperationException.class);

        ((ObjectNode) definition.tasks().get(0).config()).put("durationMs", 999);
        assertThat(definition.tasks().get(0).config().get("durationMs").asInt()).isEqualTo(10);
    }

    @Test
    @DisplayName("T01.extra-3 records with equal content are equal (value semantics, including JSON config)")
    void valueEquality() {
        WorkflowDefinition first = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).config(JsonNodeFactory.instance.objectNode().put("durationMs", 10)).build();
        WorkflowDefinition second = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).config(JsonNodeFactory.instance.objectNode().put("durationMs", 10)).build();

        assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
    }
}

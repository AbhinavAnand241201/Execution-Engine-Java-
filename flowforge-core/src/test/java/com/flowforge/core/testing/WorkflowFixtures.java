package com.flowforge.core.testing;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Canonical workflow shapes shared by every test, so "the diamond" means the same graph everywhere.
 * All tasks are DELAY tasks with {@code durationMs: 0} unless stated otherwise.
 */
public final class WorkflowFixtures {

    private WorkflowFixtures() {
    }

    /** {@code t0 → t1 → … → t(n-1)}. */
    public static WorkflowDefinition linear(int n) {
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        for (int i = 0; i < n; i++) {
            delay(builder, "t" + i);
            if (i > 0) {
                builder.dependsOn("t" + (i - 1));
            }
        }
        return builder.build();
    }

    /** {@code a → (b, c, d) → e}: the smallest graph with fan-out and a join. */
    public static WorkflowDefinition diamond() {
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        delay(builder, "a");
        delay(builder, "b").dependsOn("a");
        delay(builder, "c").dependsOn("a");
        delay(builder, "d").dependsOn("a");
        delay(builder, "e").dependsOn("b", "c", "d");
        return builder.build();
    }

    /** {@code root → (leaf0 … leaf(n-1))}. */
    public static WorkflowDefinition fanOut(int n) {
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        delay(builder, "root");
        for (int i = 0; i < n; i++) {
            delay(builder, "leaf" + i).dependsOn("root");
        }
        return builder.build();
    }

    /** {@code levels} layers of {@code width} tasks; every task depends on every task of the layer before. */
    public static WorkflowDefinition wide(int levels, int width) {
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        for (int level = 0; level < levels; level++) {
            for (int i = 0; i < width; i++) {
                delay(builder, "l" + level + "-" + i);
                if (level > 0) {
                    String[] previous = new String[width];
                    for (int p = 0; p < width; p++) {
                        previous[p] = "l" + (level - 1) + "-" + p;
                    }
                    builder.dependsOn(previous);
                }
            }
        }
        return builder.build();
    }

    /** The reference example: fetch-data (DATABASE) → generate-report (REPORT) → notify (HTTP). */
    public static WorkflowDefinition monthlyReport() {
        JsonNodeFactory json = JsonNodeFactory.instance;
        return WorkflowDefinition.builder()
            .task("fetch-data", TaskType.DATABASE)
            .config(json.objectNode().put("query", "orders_by_status").set("params", json.objectNode().put("status", "PAID")))
            .task("generate-report", TaskType.REPORT).dependsOn("fetch-data")
            .config(json.objectNode().put("source", "fetch-data").put("groupBy", "customer"))
            .task("notify", TaskType.HTTP).dependsOn("generate-report")
            .config(json.objectNode().put("method", "POST").put("url", "http://wiremock:8080/api/notify"))
            .build();
    }

    /**
     * A random DAG of {@code n} tasks: each task may depend on any earlier task (so it's acyclic by construction),
     * listed in shuffled order with shuffled key names so tests can't rely on input order.
     */
    public static WorkflowDefinition randomDag(long seed, int n) {
        Random random = new Random(seed);
        List<String> names = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            names.add("n" + i);
        }
        Collections.shuffle(names, random);

        List<Integer> order = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            order.add(i);
        }
        Collections.shuffle(order, random);

        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        for (int i : order) {
            delay(builder, names.get(i));
            List<String> deps = new ArrayList<>();
            for (int earlier = 0; earlier < i; earlier++) {
                if (random.nextInt(4) == 0) {
                    deps.add(names.get(earlier));
                }
            }
            builder.dependsOn(deps.toArray(String[]::new));
        }
        return builder.build();
    }

    private static WorkflowDefinition.Builder delay(WorkflowDefinition.Builder builder, String key) {
        return builder.task(key, TaskType.DELAY).config(JsonNodeFactory.instance.objectNode().put("durationMs", 0));
    }
}

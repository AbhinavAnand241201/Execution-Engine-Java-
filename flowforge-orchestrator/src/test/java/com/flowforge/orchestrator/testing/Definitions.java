package com.flowforge.orchestrator.testing;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Workflow definitions as request-body maps (the JSON clients send), plus unique workflow names. */
public final class Definitions {

    private Definitions() {
    }

    public static String uniqueName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public static Map<String, Object> delay(String key, long durationMs, String... dependsOn) {
        return Map.of("key", key, "type", "DELAY", "config", Map.of("durationMs", durationMs), "dependsOn", List.of(dependsOn));
    }

    /** a → (b, c, d) → e */
    public static Map<String, Object> diamond() {
        return Map.of("tasks", List.of(delay("a", 0), delay("b", 0, "a"), delay("c", 0, "a"), delay("d", 0, "a"),
            delay("e", 0, "b", "c", "d")));
    }

    public static Map<String, Object> linear(int n) {
        Object[] tasks = new Object[n];
        for (int i = 0; i < n; i++) {
            tasks[i] = i == 0 ? delay("t0", 0) : delay("t" + i, 0, "t" + (i - 1));
        }
        return Map.of("tasks", List.of(tasks));
    }

    public static Map<String, Object> createRequest(String name, Map<String, Object> definition) {
        return Map.of("name", name, "description", "test workflow", "definition", definition);
    }
}

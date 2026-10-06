package com.flowforge.core.workflow;

/**
 * How urgently a ready task should be dispatched. The numeric weight is what gets stored and sorted on
 * ({@code task_executions.priority}), so ordering is a plain {@code ORDER BY priority DESC} in SQL.
 */
public enum Priority {
    CRITICAL(40),
    HIGH(30),
    NORMAL(20),
    LOW(10);

    private final int weight;

    Priority(int weight) {
        this.weight = weight;
    }

    public int weight() {
        return weight;
    }
}

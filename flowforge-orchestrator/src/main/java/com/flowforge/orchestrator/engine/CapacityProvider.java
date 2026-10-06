package com.flowforge.orchestrator.engine;

import com.flowforge.core.workflow.TaskType;

/**
 * How many more tasks may be dispatched right now (port). Dispatch asks once per batch and takes from the
 * budget per task, so in-flight work (QUEUED + RUNNING) never exceeds what the runners can hold.
 */
@FunctionalInterface
public interface CapacityProvider {

    Budget budget();

    /** One batch's remaining capacity. */
    @FunctionalInterface
    interface Budget {
        /** Takes one slot for a task of this type; false if none is left. */
        boolean tryTake(TaskType type);

        /** Returns a slot taken by {@link #tryTake} that wasn't used after all. */
        default void giveBack(TaskType type) {
        }
    }
}

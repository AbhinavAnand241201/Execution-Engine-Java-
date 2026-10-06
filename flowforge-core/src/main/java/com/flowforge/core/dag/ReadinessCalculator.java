package com.flowforge.core.dag;

import com.flowforge.core.state.TaskStatus;
import java.util.List;
import java.util.Map;

/**
 * Answers "which tasks can start now that this one succeeded?". Only the dependents of the completed task can
 * change, so only they are checked: a dependent becomes ready when it is still PENDING and every one of its
 * dependencies has SUCCEEDED.
 *
 * <p>This is a pure function of the statuses it is given. A join becomes READY exactly once only if the caller
 * evaluates it and applies the result inside the workflow-locked transaction and moves the
 * returned tasks out of PENDING with a conditional UPDATE. Two sibling completions reading the same snapshot without
 * that lock would both be told the join is ready.
 *
 * <p>Cost: O(sum of the dependents' in-degrees) per completion. The O(1)-per-edge alternative keeps a counter of
 * unfinished dependencies per task and decrements it; it isn't needed here because workflows are capped at 200
 * tasks, and a counter would be extra state to keep consistent with the database under crashes and retries,
 * whereas this check is recomputed from the statuses that are already the source of truth.
 */
public final class ReadinessCalculator {

    private final Dag dag;

    public ReadinessCalculator(Dag dag) {
        this.dag = dag;
    }

    /**
     * Dependents of {@code completedKey} that are now ready, sorted by key. Exactly-once requires the caller to hold
     * the workflow lock and persist PENDING → READY before the next call (see the class comment).
     */
    public List<String> newlyReady(String completedKey, Map<String, TaskStatus> statuses) {
        return dag.dependents(completedKey).stream()
            .filter(dependent -> statuses.get(dependent) == TaskStatus.PENDING)
            .filter(dependent -> dag.dependencies(dependent).stream()
                .allMatch(dependency -> statuses.get(dependency) == TaskStatus.SUCCEEDED))
            .sorted()
            .toList();
    }
}

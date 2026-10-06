package com.flowforge.core.dag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Orders tasks so every task comes after its dependencies, using Kahn's algorithm. Ties are broken by task key with
 * a priority queue, so the same graph always produces the same order no matter how the definition lists its tasks;
 * that makes timelines, tests and API output reproducible. O((V + E) log V).
 */
public final class TopologicalSorter {

    private final CycleDetector cycleDetector = new CycleDetector();

    /** A single valid execution order; throws {@link CycleDetectedException} if there is none. */
    public List<String> sort(Dag dag) {
        Map<String, Integer> remaining = inDegrees(dag);
        PriorityQueue<String> ready = new PriorityQueue<>(dag.roots());
        List<String> order = new ArrayList<>(dag.size());
        while (!ready.isEmpty()) {
            String key = ready.poll();
            order.add(key);
            for (String dependent : dag.dependents(key)) {
                if (remaining.merge(dependent, -1, Integer::sum) == 0) {
                    ready.add(dependent);
                }
            }
        }
        requireComplete(dag, order.size());
        return List.copyOf(order);
    }

    /**
     * Groups tasks into levels: everything in level n depends only on levels before it, so each level could run in
     * parallel. Each level is sorted by key. Throws {@link CycleDetectedException} if the graph has a cycle.
     */
    public List<List<String>> levels(Dag dag) {
        Map<String, Integer> remaining = inDegrees(dag);
        List<List<String>> levels = new ArrayList<>();
        List<String> current = dag.roots();
        int processed = 0;
        while (!current.isEmpty()) {
            levels.add(current);
            processed += current.size();
            List<String> next = new ArrayList<>();
            for (String key : current) {
                for (String dependent : dag.dependents(key)) {
                    if (remaining.merge(dependent, -1, Integer::sum) == 0) {
                        next.add(dependent);
                    }
                }
            }
            next.sort(null);
            current = List.copyOf(next);
        }
        requireComplete(dag, processed);
        return List.copyOf(levels);
    }

    private static Map<String, Integer> inDegrees(Dag dag) {
        Map<String, Integer> inDegrees = new HashMap<>();
        for (String key : dag.keys()) {
            inDegrees.put(key, dag.inDegree(key));
        }
        return inDegrees;
    }

    private void requireComplete(Dag dag, int processed) {
        if (processed < dag.size()) {
            throw new CycleDetectedException(cycleDetector.findCycle(dag).orElseThrow());
        }
    }
}

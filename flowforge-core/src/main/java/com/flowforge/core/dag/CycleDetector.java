package com.flowforge.core.dag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Finds one cycle, if any, and returns its path so the user sees <em>which</em> tasks loop, not just that one does.
 *
 * <p>Two steps: Kahn's algorithm cheaply decides whether a cycle exists (it can't process every node), then an
 * iterative depth-first search over the nodes Kahn couldn't process extracts one concrete cycle. The DFS keeps its
 * own stack instead of recursing, so a 10 000-task chain can't overflow the thread stack.
 */
public final class CycleDetector {

    private enum Color { WHITE, GRAY, BLACK }

    /** Returns a cycle as a closed path such as {@code [a, b, c, a]}, or empty if the graph is acyclic. */
    public Optional<List<String>> findCycle(Dag dag) {
        List<String> blocked = nodesKahnCannotProcess(dag);
        if (blocked.isEmpty()) {
            return Optional.empty();
        }
        // Sorted start nodes plus the Dag's sorted neighbour lists: the same graph reports the same cycle, whatever the
        // order its tasks were listed in.
        Collections.sort(blocked);
        Map<String, Color> colors = new HashMap<>();
        for (String start : blocked) {
            if (colors.getOrDefault(start, Color.WHITE) == Color.WHITE) {
                Optional<List<String>> cycle = depthFirst(dag, start, colors);
                if (cycle.isPresent()) {
                    return cycle;
                }
            }
        }
        throw new IllegalStateException("Kahn found a cycle but DFS did not; this is a bug");
    }

    /** Renders a cycle path for error messages: {@code a → b → c → a}. */
    public static String render(List<String> cycle) {
        return String.join(" → ", cycle);
    }

    private static List<String> nodesKahnCannotProcess(Dag dag) {
        Map<String, Integer> remaining = new HashMap<>();
        Deque<String> ready = new ArrayDeque<>();
        for (String key : dag.keys()) {
            remaining.put(key, dag.inDegree(key));
            if (dag.inDegree(key) == 0) {
                ready.add(key);
            }
        }
        while (!ready.isEmpty()) {
            String key = ready.poll();
            remaining.remove(key);
            for (String dependent : dag.dependents(key)) {
                int left = remaining.merge(dependent, -1, Integer::sum);
                if (left == 0) {
                    ready.add(dependent);
                }
            }
        }
        return new ArrayList<>(remaining.keySet());
    }

    /**
     * White = unvisited, gray = on the current path, black = fully explored. Reaching a gray node means the path
     * from that node to here, plus the edge back, is a cycle.
     */
    private static Optional<List<String>> depthFirst(Dag dag, String start, Map<String, Color> colors) {
        Deque<String> path = new ArrayDeque<>();
        Deque<Integer> nextChild = new ArrayDeque<>();
        path.push(start);
        nextChild.push(0);
        colors.put(start, Color.GRAY);

        while (!path.isEmpty()) {
            String node = path.peek();
            int childIndex = nextChild.pop();
            List<String> children = dag.dependents(node);
            if (childIndex == children.size()) {
                colors.put(node, Color.BLACK);
                path.pop();
                continue;
            }
            nextChild.push(childIndex + 1);
            String child = children.get(childIndex);
            Color color = colors.getOrDefault(child, Color.WHITE);
            if (color == Color.GRAY) {
                return Optional.of(closedPath(path, child));
            }
            if (color == Color.WHITE) {
                colors.put(child, Color.GRAY);
                path.push(child);
                nextChild.push(0);
            }
        }
        return Optional.empty();
    }

    /** The current DFS path from {@code repeated} to the top, closed with {@code repeated} again. */
    private static List<String> closedPath(Deque<String> path, String repeated) {
        List<String> fromRoot = new ArrayList<>(path);
        Collections.reverse(fromRoot);
        List<String> cycle = new ArrayList<>(fromRoot.subList(fromRoot.indexOf(repeated), fromRoot.size()));
        cycle.add(repeated);
        return List.copyOf(cycle);
    }
}

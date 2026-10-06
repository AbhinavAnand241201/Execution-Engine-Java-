package com.flowforge.core.dag;

import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The dependency graph of one workflow, built once (O(V + E log V), because neighbour lists are sorted) and then
 * only read. Every graph question the engine asks (who does this task wait for, who waits for it, where do we
 * start) is a map lookup here, so the engine never re-scans the definition. Immutable, so one instance can be
 * cached per published version and shared by threads.
 *
 * <p>Edges point in execution order: if {@code b} depends on {@code a}, the edge is {@code a → b}. A {@code Dag} may
 * contain a cycle; {@link CycleDetector} finds it and {@link TopologicalSorter} refuses to sort it.
 */
public final class Dag {

    private final List<String> keys;
    private final List<String> roots;
    private final Map<String, List<String>> dependencies;
    private final Map<String, List<String>> dependents;

    private Dag(List<String> keys, Map<String, List<String>> dependencies, Map<String, List<String>> dependents) {
        this.keys = List.copyOf(keys);
        Map<String, List<String>> deps = new LinkedHashMap<>();
        Map<String, List<String>> outs = new LinkedHashMap<>();
        // Neighbour lists are sorted so every traversal (and every reported cycle) is independent of task order.
        for (String key : keys) {
            deps.put(key, dependencies.get(key).stream().sorted().toList());
            outs.put(key, dependents.get(key).stream().sorted().toList());
        }
        this.dependencies = Map.copyOf(deps);
        this.dependents = Map.copyOf(outs);
        this.roots = keys.stream().filter(key -> deps.get(key).isEmpty()).sorted().toList();
    }

    /**
     * Builds the graph of a structurally valid definition (unique keys, known and non-repeated dependencies). Throws
     * {@link IllegalArgumentException} otherwise: callers must validate first, and a bad graph here is a bug.
     */
    public static Dag of(WorkflowDefinition definition) {
        return build(definition, true, true);
    }

    /**
     * Builds whatever graph can be built from a definition that may have structural errors: duplicate keys keep the
     * first task, unknown and repeated dependencies are skipped, and self edges are kept only if asked. Used by
     * {@link DagValidator}, whose structural errors are already reported by {@code WorkflowDefinitionValidator}.
     */
    static Dag lenient(WorkflowDefinition definition, boolean keepSelfEdges) {
        return build(definition, false, keepSelfEdges);
    }

    private static Dag build(WorkflowDefinition definition, boolean strict, boolean keepSelfEdges) {
        List<String> keys = new ArrayList<>();
        Map<String, List<String>> dependencies = new LinkedHashMap<>();
        Map<String, List<String>> dependents = new LinkedHashMap<>();
        for (TaskDefinition task : definition.tasks()) {
            if (task.key() == null || dependencies.containsKey(task.key())) {
                if (strict) {
                    throw new IllegalArgumentException("missing or duplicate task key: " + task.key());
                }
                continue;
            }
            keys.add(task.key());
            dependencies.put(task.key(), new ArrayList<>());
            dependents.put(task.key(), new ArrayList<>());
        }

        Set<String> seenTasks = new HashSet<>();
        for (TaskDefinition task : definition.tasks()) {
            if (task.key() == null || !seenTasks.add(task.key())) {
                continue;
            }
            Set<String> seenDeps = new HashSet<>();
            for (String dependency : task.dependsOn()) {
                boolean known = dependencies.containsKey(dependency);
                boolean self = dependency.equals(task.key());
                boolean repeated = !seenDeps.add(dependency);
                if (strict && (!known || repeated)) {
                    throw new IllegalArgumentException("unknown or repeated dependency '" + dependency + "' of task '"
                        + task.key() + "'");
                }
                if (!known || repeated || (self && !keepSelfEdges)) {
                    continue;
                }
                dependencies.get(task.key()).add(dependency);
                dependents.get(dependency).add(task.key());
            }
        }
        return new Dag(keys, dependencies, dependents);
    }

    /** Task keys in definition order. */
    public List<String> keys() {
        return keys;
    }

    public int size() {
        return keys.size();
    }

    /** Tasks that {@code key} waits for, sorted by key. */
    public List<String> dependencies(String key) {
        return require(dependencies, key);
    }

    /** Tasks that wait for {@code key}, sorted by key. */
    public List<String> dependents(String key) {
        return require(dependents, key);
    }

    public int inDegree(String key) {
        return dependencies(key).size();
    }

    /** Tasks with no dependencies, sorted by key: they are READY as soon as an execution starts. */
    public List<String> roots() {
        return roots;
    }

    private static List<String> require(Map<String, List<String>> map, String key) {
        List<String> value = map.get(key);
        if (value == null) {
            throw new IllegalArgumentException("unknown task key: " + key);
        }
        return value;
    }
}

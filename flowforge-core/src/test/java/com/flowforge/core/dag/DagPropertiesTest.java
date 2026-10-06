package com.flowforge.core.dag;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import net.jqwik.api.ForAll;
import net.jqwik.api.Label;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

/** Property tests (jqwik); {@code @Label} carries the test ID because jqwik does not read {@code @DisplayName}. */
class DagPropertiesTest {

    private final TopologicalSorter sorter = new TopologicalSorter();
    private final CycleDetector detector = new CycleDetector();

    @Property(tries = 300)
    @Label("T02.9 random DAG: for every edge u → v, u comes before v in the topological order")
    void everyEdgeRespected(@ForAll long seed, @ForAll @IntRange(min = 1, max = 80) int size) {
        WorkflowDefinition definition = WorkflowFixtures.randomDag(seed, size);
        Dag dag = Dag.of(definition);

        List<String> order = sorter.sort(dag);
        Map<String, Integer> position = new HashMap<>();
        for (int i = 0; i < order.size(); i++) {
            position.put(order.get(i), i);
        }

        assertThat(order).hasSize(size).doesNotHaveDuplicates();
        for (TaskDefinition task : definition.tasks()) {
            for (String dependency : task.dependsOn()) {
                assertThat(position.get(dependency)).isLessThan(position.get(task.key()));
            }
        }
        assertThat(detector.findCycle(dag)).isEmpty();
    }

    @Property(tries = 300)
    @Label("T02.9 random DAG plus one back edge: the cycle is detected and the reported path is a real cycle")
    void backEdgeCreatesDetectedCycle(@ForAll long seed, @ForAll @IntRange(min = 2, max = 80) int size) {
        WorkflowDefinition definition = WorkflowFixtures.randomDag(seed, size);
        Random random = new Random(seed);

        // Pick u and a v reachable from u (u ⇝ v); making u depend on v closes a cycle. A self-pair can't happen.
        Dag dag = Dag.of(definition);
        List<String> keys = new ArrayList<>(dag.keys());
        String u = null;
        String v = null;
        for (int attempt = 0; attempt < keys.size() && v == null; attempt++) {
            String candidate = keys.get(random.nextInt(keys.size()));
            List<String> reachable = new ArrayList<>(reachableFrom(dag, candidate));
            if (!reachable.isEmpty()) {
                u = candidate;
                v = reachable.get(random.nextInt(reachable.size()));
            }
        }
        if (v == null) {
            // No edges at all: make any two nodes depend on each other.
            u = keys.get(0);
            v = keys.get(1);
            definition = withExtraDependency(definition, v, u);
        }
        WorkflowDefinition cyclic = withExtraDependency(definition, u, v);

        List<String> cycle = detector.findCycle(Dag.of(cyclic)).orElseThrow();

        assertThat(cycle.get(0)).isEqualTo(cycle.get(cycle.size() - 1));
        // A simple cycle: no node repeats except the closing one, and the fixture has no self edges.
        assertThat(cycle.subList(0, cycle.size() - 1)).doesNotHaveDuplicates();
        assertThat(cycle).hasSizeGreaterThanOrEqualTo(3);
        Dag cyclicDag = Dag.of(cyclic);
        for (int i = 0; i + 1 < cycle.size(); i++) {
            assertThat(cyclicDag.dependents(cycle.get(i))).contains(cycle.get(i + 1));
        }
    }

    private static Set<String> reachableFrom(Dag dag, String start) {
        Set<String> seen = new HashSet<>();
        Deque<String> todo = new ArrayDeque<>(dag.dependents(start));
        while (!todo.isEmpty()) {
            String next = todo.pop();
            if (seen.add(next)) {
                todo.addAll(dag.dependents(next));
            }
        }
        return seen;
    }

    private static WorkflowDefinition withExtraDependency(WorkflowDefinition definition, String task, String dependsOn) {
        List<TaskDefinition> tasks = new ArrayList<>();
        for (TaskDefinition t : definition.tasks()) {
            if (t.key().equals(task) && !t.dependsOn().contains(dependsOn)) {
                List<String> deps = new ArrayList<>(t.dependsOn());
                deps.add(dependsOn);
                t = new TaskDefinition(t.key(), t.type(), t.config(), deps, t.timeout(), t.priority(), t.retry());
            }
            tasks.add(t);
        }
        return new WorkflowDefinition(tasks);
    }
}

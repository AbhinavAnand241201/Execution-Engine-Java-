package com.flowforge.core.dag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TopologicalSorterTest {

    private final TopologicalSorter sorter = new TopologicalSorter();

    @Test
    @DisplayName("T02.1 linear chain sorts as a, b, c")
    void linearChain() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("c", TaskType.DELAY).dependsOn("b")
            .task("a", TaskType.DELAY)
            .task("b", TaskType.DELAY).dependsOn("a")
            .build();

        assertThat(sorter.sort(Dag.of(definition))).containsExactly("a", "b", "c");
    }

    @Test
    @DisplayName("T02.2 diamond levels are [[a], [b, c, d], [e]]")
    void diamondLevels() {
        assertThat(sorter.levels(Dag.of(WorkflowFixtures.diamond())))
            .containsExactly(List.of("a"), List.of("b", "c", "d"), List.of("e"));
    }

    @Test
    @DisplayName("T02.5 same input gives an identical order across 100 runs and shuffled input order")
    void deterministicOrder() {
        WorkflowDefinition definition = WorkflowFixtures.randomDag(42, 60);
        List<String> expected = sorter.sort(Dag.of(definition));
        List<List<String>> expectedLevels = sorter.levels(Dag.of(definition));
        Random random = new Random(7);

        for (int run = 0; run < 100; run++) {
            List<TaskDefinition> shuffled = new ArrayList<>(definition.tasks());
            Collections.shuffle(shuffled, random);
            Dag dag = Dag.of(new WorkflowDefinition(shuffled));

            assertThat(sorter.sort(dag)).isEqualTo(expected);
            assertThat(sorter.levels(dag)).isEqualTo(expectedLevels);
        }
    }

    @Test
    @DisplayName("T02.7 multiple roots and isolated nodes are all roots, in key order")
    void multipleRootsAndIsolatedNodes() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("z-isolated", TaskType.DELAY)
            .task("b", TaskType.DELAY)
            .task("a", TaskType.DELAY)
            .task("join", TaskType.DELAY).dependsOn("a", "b")
            .build();
        Dag dag = Dag.of(definition);

        assertThat(dag.roots()).containsExactly("a", "b", "z-isolated");
        assertThat(sorter.levels(dag)).containsExactly(List.of("a", "b", "z-isolated"), List.of("join"));
    }

    @Test
    @DisplayName("T02.extra-1 sorting a graph with a cycle throws CycleDetectedException with the path")
    void sortRejectsCycle() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).dependsOn("b")
            .task("b", TaskType.DELAY).dependsOn("a")
            .build();

        assertThatThrownBy(() -> sorter.sort(Dag.of(definition)))
            .isInstanceOf(CycleDetectedException.class)
            .hasMessageContaining("a → b → a");
    }

    @Test
    @DisplayName("T02.extra-2 Dag exposes dependencies, dependents and in-degree; unknown or repeated dependency is rejected")
    void dagAccessors() {
        Dag dag = Dag.of(WorkflowFixtures.diamond());

        assertThat(dag.dependencies("e")).containsExactly("b", "c", "d");
        assertThat(dag.dependents("a")).containsExactly("b", "c", "d");
        assertThat(dag.inDegree("e")).isEqualTo(3);
        assertThat(dag.size()).isEqualTo(5);
        assertThatThrownBy(() -> Dag.of(WorkflowDefinition.builder().task("a", TaskType.DELAY).dependsOn("ghost").build()))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Dag.of(WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).task("b", TaskType.DELAY).dependsOn("a", "a").build()))
            .isInstanceOf(IllegalArgumentException.class);
    }
}

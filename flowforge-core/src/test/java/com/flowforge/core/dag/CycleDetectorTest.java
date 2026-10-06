package com.flowforge.core.dag;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CycleDetectorTest {

    private final CycleDetector detector = new CycleDetector();

    @Test
    @DisplayName("T02.3 self loop is detected as a → a")
    void selfLoop() {
        Dag dag = Dag.lenient(WorkflowDefinition.builder().task("a", TaskType.DELAY).dependsOn("a").build(), true);

        assertThat(detector.findCycle(dag)).contains(List.of("a", "a"));
    }

    @Test
    @DisplayName("T02.3 2-cycle is detected with its path")
    void twoCycle() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).dependsOn("b")
            .task("b", TaskType.DELAY).dependsOn("a")
            .build();

        assertThat(detector.findCycle(Dag.of(definition))).contains(List.of("a", "b", "a"));
    }

    @Test
    @DisplayName("T02.3 3-cycle is detected and the message renders the path a → b → c → a")
    void threeCycle() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("root", TaskType.DELAY)
            .task("a", TaskType.DELAY).dependsOn("root", "c")
            .task("b", TaskType.DELAY).dependsOn("a")
            .task("c", TaskType.DELAY).dependsOn("b")
            .build();

        List<String> cycle = detector.findCycle(Dag.of(definition)).orElseThrow();

        assertThat(cycle).containsExactly("a", "b", "c", "a");
        assertThat(CycleDetector.render(cycle)).isEqualTo("a → b → c → a");
    }

    @Test
    @DisplayName("T02.4 a cycle inside one disconnected component is still detected")
    void cycleInDisconnectedComponent() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("x1", TaskType.DELAY)
            .task("x2", TaskType.DELAY).dependsOn("x1")
            .task("y1", TaskType.DELAY).dependsOn("y2")
            .task("y2", TaskType.DELAY).dependsOn("y1")
            .build();

        assertThat(detector.findCycle(Dag.of(definition))).contains(List.of("y1", "y2", "y1"));
    }

    @Test
    @DisplayName("T02.4 a node that depends on a cycle and sorts first is skipped; the cycle x → y → x is reported")
    void downstreamNodeSortsBeforeCycle() {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).dependsOn("x")
            .task("x", TaskType.DELAY).dependsOn("y")
            .task("y", TaskType.DELAY).dependsOn("x")
            .build();

        assertThat(detector.findCycle(Dag.of(definition))).contains(List.of("x", "y", "x"));
    }

    @Test
    @DisplayName("T02.5 the reported cycle does not depend on the order tasks are listed in")
    void cycleReportIndependentOfTaskOrder() {
        WorkflowDefinition bFirst = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).dependsOn("c", "b")
            .task("b", TaskType.DELAY).dependsOn("a")
            .task("c", TaskType.DELAY).dependsOn("a")
            .build();
        WorkflowDefinition cFirst = WorkflowDefinition.builder()
            .task("c", TaskType.DELAY).dependsOn("a")
            .task("b", TaskType.DELAY).dependsOn("a")
            .task("a", TaskType.DELAY).dependsOn("b", "c")
            .build();

        assertThat(detector.findCycle(Dag.of(bFirst))).contains(List.of("a", "b", "a"));
        assertThat(detector.findCycle(Dag.of(cFirst))).contains(List.of("a", "b", "a"));
    }

    @Test
    @DisplayName("T02.extra-7 empty graph: no cycle, empty order, no levels")
    void emptyGraph() {
        Dag empty = Dag.of(new WorkflowDefinition(List.of()));

        assertThat(detector.findCycle(empty)).isEmpty();
        assertThat(new TopologicalSorter().sort(empty)).isEmpty();
        assertThat(new TopologicalSorter().levels(empty)).isEmpty();
    }

    @Test
    @DisplayName("T02.extra-3 acyclic graphs report no cycle")
    void acyclic() {
        assertThat(detector.findCycle(Dag.of(WorkflowFixtures.diamond()))).isEmpty();
        assertThat(detector.findCycle(Dag.of(WorkflowFixtures.wide(4, 5)))).isEmpty();
    }
}

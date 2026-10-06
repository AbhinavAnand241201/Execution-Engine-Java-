package com.flowforge.core.dag;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.ConfigValidator;
import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.core.workflow.WorkflowDefinitionValidator;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Size guard, not a benchmark: the limit of 200 tasks per workflow is far below these sizes, which exist to prove
 * the algorithms are linear-ish and iterative. The 1 s bound is a requirement and must not be widened; if CI
 * ever flakes on it, find out why instead.
 */
class DagScaleTest {

    // Structural limit raised so the graph algorithms, not the 200-task rule, are what's exercised.
    private final DagValidator validator = new DagValidator(
        new WorkflowDefinitionValidator(ConfigValidator.ACCEPT_ALL, 20_000));
    private final TopologicalSorter sorter = new TopologicalSorter();

    @Test
    @DisplayName("T02.8 10 000-node chain: validate + sort in under 1 s, no StackOverflowError")
    void longChain() {
        WorkflowDefinition chain = WorkflowFixtures.linear(10_000);

        long start = System.nanoTime();
        assertThat(validator.validate(chain)).isEmpty();
        List<String> order = sorter.sort(Dag.of(chain));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(order).hasSize(10_000);
        assertThat(order.get(0)).isEqualTo("t0");
        assertThat(elapsed).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("T02.8 10 000-node cycle: detected iteratively without StackOverflowError")
    void longCycle() throws InterruptedException {
        WorkflowDefinition chain = WorkflowFixtures.linear(10_000);
        List<TaskDefinition> tasks = new ArrayList<>(chain.tasks());
        TaskDefinition first = tasks.get(0);
        tasks.set(0, new TaskDefinition(first.key(), first.type(), first.config(),
            List.of("t9999"), first.timeout(), first.priority(), first.retry()));

        Dag dag = Dag.of(new WorkflowDefinition(tasks));
        AtomicReference<Object> outcome = new AtomicReference<>();

        // A 256 KB stack: a recursive DFS 10 000 frames deep would overflow here, so passing proves it's iterative.
        Thread smallStack = new Thread(null, () -> {
            try {
                outcome.set(new CycleDetector().findCycle(dag).orElseThrow());
            } catch (Throwable t) {
                outcome.set(t);
            }
        }, "small-stack", 256 * 1024);
        smallStack.start();
        smallStack.join();

        assertThat(outcome.get()).isInstanceOfSatisfying(List.class, cycle -> assertThat(cycle).hasSize(10_001));
    }

    @Test
    @DisplayName("T02.8 5 000-wide fan-out: validate + sort in under 1 s")
    void wideFanOut() {
        WorkflowDefinition fanOut = WorkflowFixtures.fanOut(5_000);

        long start = System.nanoTime();
        assertThat(validator.validate(fanOut)).isEmpty();
        List<List<String>> levels = sorter.levels(Dag.of(fanOut));
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        assertThat(levels).hasSize(2);
        assertThat(levels.get(1)).hasSize(5_000);
        assertThat(elapsed).isLessThan(Duration.ofSeconds(1));
    }
}

package com.flowforge.core.dag;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.testing.WorkflowFixtures;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReadinessCalculatorTest {

    private final ReadinessCalculator readiness = new ReadinessCalculator(Dag.of(WorkflowFixtures.diamond()));

    @Test
    @DisplayName("T02.6 e is not ready after b or c; ready exactly once when the last of b, c, d succeeds")
    void joinBecomesReadyExactlyOnce() {
        Map<String, TaskStatus> statuses = new HashMap<>(Map.of(
            "a", TaskStatus.SUCCEEDED, "b", TaskStatus.RUNNING, "c", TaskStatus.RUNNING, "d", TaskStatus.RUNNING,
            "e", TaskStatus.PENDING));

        statuses.put("b", TaskStatus.SUCCEEDED);
        assertThat(readiness.newlyReady("b", statuses)).isEmpty();

        statuses.put("c", TaskStatus.SUCCEEDED);
        assertThat(readiness.newlyReady("c", statuses)).isEmpty();

        statuses.put("d", TaskStatus.SUCCEEDED);
        assertThat(readiness.newlyReady("d", statuses)).containsExactly("e");

        // The caller marks e READY; a duplicate completion of d must not report it again.
        statuses.put("e", TaskStatus.READY);
        assertThat(readiness.newlyReady("d", statuses)).isEmpty();
    }

    @Test
    @DisplayName("T02.6 fan-out: completing the root makes all its dependents ready, in key order")
    void fanOutAfterRoot() {
        Map<String, TaskStatus> statuses = new HashMap<>(Map.of(
            "a", TaskStatus.SUCCEEDED, "b", TaskStatus.PENDING, "c", TaskStatus.PENDING, "d", TaskStatus.PENDING,
            "e", TaskStatus.PENDING));

        assertThat(readiness.newlyReady("a", statuses)).isEqualTo(List.of("b", "c", "d"));
    }

    @Test
    @DisplayName("T02.extra-4 a dependency that failed or was cancelled never makes the join ready")
    void failedDependencyBlocksJoin() {
        Map<String, TaskStatus> statuses = Map.of(
            "a", TaskStatus.SUCCEEDED, "b", TaskStatus.SUCCEEDED, "c", TaskStatus.FAILED_PERMANENTLY,
            "d", TaskStatus.SUCCEEDED, "e", TaskStatus.PENDING);

        assertThat(readiness.newlyReady("d", statuses)).isEmpty();
    }
}

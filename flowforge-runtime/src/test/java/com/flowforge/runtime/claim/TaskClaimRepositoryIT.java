package com.flowforge.runtime.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.runtime.testing.EngineDatabase;
import com.flowforge.runtime.testing.EngineDatabase.Execution;
import com.flowforge.runtime.testing.EngineDatabase.TaskRow;
import com.flowforge.runtime.testing.IntegrationTestBase;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TaskClaimRepositoryIT extends IntegrationTestBase {

    private final TaskClaimRepository claims = new TaskClaimRepository(EngineDatabase.dataSource());
    private final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    @Test
    @DisplayName("T11.1 claiming a QUEUED task with the matching attempt → true; RUNNING with worker, started_at, deadline")
    void claim() {
        Execution execution = EngineDatabase.insertExecution("RUNNING", null, "QUEUED", 1, 5_000, "a");
        UUID task = execution.tasks()[0];

        assertThat(claims.claim(task, 1, "worker-1", now)).isTrue();

        TaskRow row = EngineDatabase.task(task);
        assertThat(row.status()).isEqualTo("RUNNING");
        assertThat(row.workerId()).isEqualTo("worker-1");
        assertThat(row.startedAt()).isEqualTo(now);
        assertThat(row.deadlineAt()).isEqualTo(now.plus(Duration.ofSeconds(5)));
    }

    @Test
    @DisplayName("T11.2 a wrong attempt → false; a task not QUEUED (READY, RUNNING, SUCCEEDED) → false; row unchanged")
    void rejected() {
        Execution execution = EngineDatabase.insertExecution("RUNNING", null, "QUEUED", 2, 5_000, "a");
        UUID task = execution.tasks()[0];
        assertThat(claims.claim(task, 1, "w", now)).isFalse();
        assertThat(claims.claim(task, 3, "w", now)).isFalse();
        assertThat(EngineDatabase.task(task).status()).isEqualTo("QUEUED");

        for (String status : List.of("READY", "RUNNING", "SUCCEEDED")) {
            EngineDatabase.setTask(task, status, null);
            assertThat(claims.claim(task, 2, "w", now)).as(status).isFalse();
            assertThat(EngineDatabase.task(task).status()).isEqualTo(status);
        }
    }

    @Test
    @DisplayName("T11.3 race: 8 threads claim the same task (start gate, 50 iterations) → exactly one true per iteration")
    void race() throws Exception {
        List<UUID> tasks = new ArrayList<>();
        List<List<Object>> outcomes = ConcurrencyHarness.race(8, 50,
            thread -> claims.claim(tasks.getLast(), 1, "worker-" + thread, now),
            () -> tasks.add(EngineDatabase.insertExecution("RUNNING", null, "QUEUED", 1, 5_000, "a").tasks()[0]));

        for (int i = 0; i < outcomes.size(); i++) {
            assertThat(outcomes.get(i)).as("iteration %d", i).allSatisfy(o -> assertThat(o).isInstanceOf(Boolean.class));
            assertThat(outcomes.get(i).stream().filter(Boolean.TRUE::equals)).as("iteration %d", i).hasSize(1);
            int winner = outcomes.get(i).indexOf(Boolean.TRUE);
            assertThat(EngineDatabase.task(tasks.get(i)).workerId()).isEqualTo("worker-" + winner);
        }
    }

    @ParameterizedTest(name = "execution {0}")
    @ValueSource(strings = {"PAUSED", "CANCELLING", "CANCELLED", "FAILED"})
    @DisplayName("T11.4 the execution is not RUNNING (PAUSED, CANCELLING, ...) → claim false, task stays QUEUED")
    void executionNotRunning(String status) {
        Execution execution = EngineDatabase.insertExecution(status, null, "QUEUED", 1, 5_000, "a");

        assertThat(claims.claim(execution.tasks()[0], 1, "w", now)).isFalse();
        assertThat(EngineDatabase.task(execution.tasks()[0]).status()).isEqualTo("QUEUED");
    }
}

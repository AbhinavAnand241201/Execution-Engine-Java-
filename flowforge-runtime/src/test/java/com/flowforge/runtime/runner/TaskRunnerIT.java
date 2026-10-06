package com.flowforge.runtime.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.core.testing.ScriptedExecutor;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.claim.TaskClaimRepository;
import com.flowforge.runtime.claim.TaskInputRepository;
import com.flowforge.runtime.executor.OutputGuard;
import com.flowforge.runtime.testing.EngineDatabase;
import com.flowforge.runtime.testing.EngineDatabase.Execution;
import com.flowforge.runtime.testing.IntegrationTestBase;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TaskRunnerIT extends IntegrationTestBase {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    @DisplayName("T11.5 duplicate commands for one attempt (8 concurrent deliveries, start gate, 50 iterations) → the "
        + "executor runs exactly once per task; the rest are ACKed as duplicates")
    void duplicateCommands() throws Exception {
        ScriptedExecutor executor = ScriptedExecutor.succeeding();
        Queue<TaskResultMessage> published = new ConcurrentLinkedQueue<>();
        try (TaskRunner runner = new TaskRunner(new TaskRunnerSettings("worker-it", 16, ThreadMode.PLATFORM,
            Duration.ofSeconds(2), 3, Duration.ofMillis(1)), new TaskExecutorRegistry(List.of(executor)),
            new TaskClaimRepository(EngineDatabase.dataSource()), new TaskInputRepository(EngineDatabase.dataSource(), JSON),
            published::add, new OutputGuard(65_536), Clock.systemUTC())) {
            List<TaskCommand> commands = new ArrayList<>();

            List<List<Object>> outcomes = ConcurrencyHarness.race(8, 50, thread -> {
                try {
                    return runner.onCommand(commands.getLast());
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }, () -> {
                String key = "dup-" + commands.size();
                Execution execution = EngineDatabase.insertExecution("RUNNING", null, "QUEUED", 1, 5_000, key);
                commands.add(new TaskCommand(UUID.randomUUID(), 1, execution.tasks()[0], execution.id(), key,
                    TaskType.DELAY, 1, JSON.createObjectNode(), List.of(), 5_000, "corr", Instant.now()));
            });

            await().atMost(Duration.ofSeconds(10)).until(() -> published.size() == 50 && runner.inFlight() == 0);
            for (int i = 0; i < 50; i++) {
                assertThat(outcomes.get(i)).as("iteration %d", i).containsOnly(CommandOutcome.ACK);
                assertThat(executor.invocations("dup-" + i)).as("iteration %d", i).isEqualTo(1);
                assertThat(EngineDatabase.task(commands.get(i).taskExecutionId()).workerId()).isEqualTo("worker-it");
            }
            assertThat(runner.duplicateOrStaleClaims()).isEqualTo(50 * 7);
            assertThat(published.stream().map(TaskResultMessage::taskExecutionId).distinct()).hasSize(50);
        }
    }
}

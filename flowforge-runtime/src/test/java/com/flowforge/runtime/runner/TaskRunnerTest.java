package com.flowforge.runtime.runner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.testing.Behaviours;
import com.flowforge.core.testing.ScriptedExecutor;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.claim.TaskClaimRepository;
import com.flowforge.runtime.claim.TaskInputRepository;
import com.flowforge.runtime.executor.OutputGuard;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** TaskRunner without a database: claims always win (or are scripted), inputs are empty. */
class TaskRunnerTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    private final Queue<TaskResultMessage> published = new ConcurrentLinkedQueue<>();
    private TaskRunner runner;

    @AfterEach
    void stop() throws InterruptedException {
        if (runner != null) {
            runner.close();
        }
    }

    private TaskRunner runner(ScriptedExecutor executor, int maxConcurrency, ThreadMode mode, TaskResultSink sink,
                              int publishAttempts) {
        TaskClaimRepository alwaysWins = new TaskClaimRepository(null) {
            @Override
            public boolean claim(UUID taskExecutionId, int attempt, String workerId, Instant now) {
                return true;
            }
        };
        TaskInputRepository noInputs = new TaskInputRepository(null, JSON) {
            @Override
            public Map<String, JsonNode> load(TaskCommand command) {
                return Map.of();
            }
        };
        runner = new TaskRunner(new TaskRunnerSettings("worker-t", maxConcurrency, mode, Duration.ofMillis(50),
            publishAttempts, Duration.ofMillis(1)), new TaskExecutorRegistry(List.of(executor)), alwaysWins, noInputs,
            sink, new OutputGuard(65_536), Clock.systemUTC());
        return runner;
    }

    private TaskRunner runner(ScriptedExecutor executor, int maxConcurrency) {
        return runner(executor, maxConcurrency, ThreadMode.PLATFORM, published::add, 3);
    }

    private static TaskCommand command(String key, long timeoutMs) {
        return new TaskCommand(UUID.randomUUID(), 1, UUID.randomUUID(), UUID.randomUUID(), key, TaskType.DELAY, 1,
            JSON.createObjectNode(), List.of(), timeoutMs, "corr", Instant.now());
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ThreadMode.class)
    @DisplayName("T11.extra-2 a successful attempt is published once as SUCCEEDED with output, worker and timestamps")
    void success(ThreadMode mode) throws Exception {
        runner(ScriptedExecutor.succeeding(), 2, mode, published::add, 3);
        TaskCommand command = command("a", 5_000);

        assertThat(runner.onCommand(command)).isEqualTo(CommandOutcome.ACK);

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1 && runner.inFlight() == 0);
        TaskResultMessage result = published.peek();
        assertThat(result.outcome()).isEqualTo(AttemptOutcome.SUCCEEDED);
        assertThat(result.taskExecutionId()).isEqualTo(command.taskExecutionId());
        assertThat(result.attempt()).isEqualTo(1);
        assertThat(result.workerId()).isEqualTo("worker-t");
        assertThat(result.finishedAt()).isAfterOrEqualTo(result.startedAt());
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ThreadMode.class)
    @DisplayName("T11.6 an executor exceeding its timeout is interrupted → TIMED_OUT retryable result, permit released")
    void timeout(ThreadMode mode) throws Exception {
        AtomicBoolean interrupted = new AtomicBoolean();
        runner(new ScriptedExecutor(TaskType.DELAY, Behaviours.slow(Duration.ofSeconds(30), true, interrupted)), 1, mode,
            published::add, 3);

        runner.onCommand(command("slow", 200));

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1 && runner.inFlight() == 0);
        TaskResultMessage result = published.peek();
        assertThat(result.outcome()).isEqualTo(AttemptOutcome.TIMED_OUT);
        assertThat(result.retryable()).isTrue();
        assertThat(result.errorCode()).isEqualTo("TIMEOUT");
        assertThat(interrupted).isTrue();
        // The permit is back: the next command is accepted.
        assertThat(runner.onCommand(command("slow", 200))).isEqualTo(CommandOutcome.ACK);
    }

    @Test
    @DisplayName("T11.6 an executor that ignores the interrupt still reports TIMED_OUT, whatever it returns later")
    void timeoutIgnoringInterrupt() throws Exception {
        AtomicBoolean interrupted = new AtomicBoolean();
        runner(new ScriptedExecutor(TaskType.DELAY, Behaviours.slow(Duration.ofMillis(600), false, interrupted)), 1);

        runner.onCommand(command("stubborn", 100));

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1);
        assertThat(published.peek().outcome()).isEqualTo(AttemptOutcome.TIMED_OUT);
        await().atMost(Duration.ofSeconds(5)).until(() -> runner.inFlight() == 0);
        assertThat(published).hasSize(1);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ThreadMode.class)
    @DisplayName("T11.7 max-concurrency 2 → a third command gets RETRY_LATER until a permit is free")
    void capacity(ThreadMode mode) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        runner(new ScriptedExecutor(TaskType.DELAY, Behaviours.latch(release)), 2, mode, published::add, 3);

        assertThat(runner.onCommand(command("a", 10_000))).isEqualTo(CommandOutcome.ACK);
        assertThat(runner.onCommand(command("b", 10_000))).isEqualTo(CommandOutcome.ACK);
        assertThat(runner.onCommand(command("c", 10_000))).isEqualTo(CommandOutcome.RETRY_LATER);
        assertThat(runner.inFlight()).isEqualTo(2);

        release.countDown();
        await().atMost(Duration.ofSeconds(5)).until(() -> runner.inFlight() == 0);
        assertThat(runner.onCommand(command("c", 10_000))).isEqualTo(CommandOutcome.ACK);
    }

    @Test
    @DisplayName("T11.8 an executor throwing RuntimeException → EXECUTOR_CRASHED retryable; the runner keeps working")
    void crash() throws Exception {
        runner(ScriptedExecutor.succeeding().on("boom", context -> {
            throw new IllegalStateException("bug in executor");
        }), 1);

        runner.onCommand(command("boom", 5_000));
        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1 && runner.inFlight() == 0);
        assertThat(published.peek().errorCode()).isEqualTo("EXECUTOR_CRASHED");
        assertThat(published.peek().retryable()).isTrue();
        assertThat(published.peek().outcome()).isEqualTo(AttemptOutcome.FAILED);

        runner.onCommand(command("fine", 5_000));
        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 2);
        assertThat(published).last().satisfies(r -> assertThat(r.outcome()).isEqualTo(AttemptOutcome.SUCCEEDED));
    }

    @Test
    @DisplayName("T11.9 the result sink fails twice then succeeds → published exactly once; permit released")
    void sinkRetries() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        runner(ScriptedExecutor.succeeding(), 1, ThreadMode.PLATFORM, result -> {
            if (calls.incrementAndGet() <= 2) {
                throw new IllegalStateException("broker down");
            }
            published.add(result);
        }, 5);

        runner.onCommand(command("a", 5_000));

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1 && runner.inFlight() == 0);
        assertThat(calls).hasValue(3);
        assertThat(runner.droppedResults()).isZero();
    }

    @Test
    @DisplayName("T11.9 when every publish try fails the result is dropped (counted) and the permit is still released")
    void sinkAlwaysFails() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        runner(ScriptedExecutor.succeeding(), 1, ThreadMode.PLATFORM, result -> {
            calls.incrementAndGet();
            throw new IllegalStateException("broker down");
        }, 3);

        runner.onCommand(command("a", 5_000));

        await().atMost(Duration.ofSeconds(5)).until(() -> runner.droppedResults() == 1 && runner.inFlight() == 0);
        assertThat(calls).hasValue(3);
        assertThat(runner.onCommand(command("b", 5_000))).isEqualTo(CommandOutcome.ACK);
    }

    @RepeatedTest(50)
    @DisplayName("T11.extra-4 timeout racing completion (timeout ≈ run time) → exactly one result, permit back, and the "
        + "next attempt on the same thread is not interrupted")
    void timeoutRacingCompletion() throws Exception {
        AtomicBoolean ignored = new AtomicBoolean();
        AtomicBoolean secondInterrupted = new AtomicBoolean();
        runner(ScriptedExecutor.succeeding()
            .on("racy", Behaviours.slow(Duration.ofMillis(20), true, ignored))
            .on("next", Behaviours.slow(Duration.ofMillis(30), true, secondInterrupted)), 1);

        runner.onCommand(command("racy", 20));
        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1 && runner.inFlight() == 0);
        assertThat(published.peek().outcome()).isIn(AttemptOutcome.SUCCEEDED, AttemptOutcome.TIMED_OUT);

        runner.onCommand(command("next", 5_000));
        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 2 && runner.inFlight() == 0);
        assertThat(secondInterrupted).isFalse();
        assertThat(published).last().satisfies(r -> assertThat(r.outcome()).isEqualTo(AttemptOutcome.SUCCEEDED));
    }

    @Test
    @DisplayName("T11.extra-5 a claim that throws (database down) releases the permit and propagates")
    void claimThrows() throws Exception {
        TaskClaimRepository broken = new TaskClaimRepository(null) {
            @Override
            public boolean claim(UUID taskExecutionId, int attempt, String workerId, Instant now) {
                throw new IllegalStateException("database down");
            }
        };
        runner = new TaskRunner(new TaskRunnerSettings("w", 1, ThreadMode.PLATFORM, Duration.ofMillis(50), 1,
            Duration.ofMillis(1)), new TaskExecutorRegistry(List.of(ScriptedExecutor.succeeding())), broken, null,
            published::add, new OutputGuard(65_536), Clock.systemUTC());

        assertThatThrownBy(() -> runner.onCommand(command("a", 1_000))).hasMessage("database down");
        assertThat(runner.inFlight()).isZero();
    }

    @Test
    @DisplayName("T11.extra-6 after shutdown, commands get RETRY_LATER and nothing is claimed; an attempt interrupted "
        + "by shutdown is reported as retryable WORKER_SHUTDOWN")
    void shutdown() throws Exception {
        AtomicInteger claimsMade = new AtomicInteger();
        AtomicBoolean interrupted = new AtomicBoolean();
        TaskClaimRepository counting = new TaskClaimRepository(null) {
            @Override
            public boolean claim(UUID taskExecutionId, int attempt, String workerId, Instant now) {
                claimsMade.incrementAndGet();
                return true;
            }
        };
        TaskInputRepository noInputs = new TaskInputRepository(null, JSON) {
            @Override
            public Map<String, JsonNode> load(TaskCommand command) {
                return Map.of();
            }
        };
        runner = new TaskRunner(new TaskRunnerSettings("w", 2, ThreadMode.PLATFORM, Duration.ofMillis(50), 3,
            Duration.ofMillis(1)), new TaskExecutorRegistry(List.of(new ScriptedExecutor(TaskType.DELAY,
            Behaviours.slow(Duration.ofSeconds(30), true, interrupted)))), counting, noInputs, published::add,
            new OutputGuard(65_536), Clock.systemUTC());
        runner.onCommand(command("long", 60_000));
        await().atMost(Duration.ofSeconds(5)).until(() -> runner.inFlight() == 1);

        assertThat(runner.shutdown(Duration.ofMillis(100))).isFalse();

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1);
        assertThat(published.peek().outcome()).isEqualTo(AttemptOutcome.WORKER_SHUTDOWN);
        assertThat(published.peek().retryable()).isTrue();
        assertThat(runner.onCommand(command("late", 1_000))).isEqualTo(CommandOutcome.RETRY_LATER);
        assertThat(claimsMade).hasValue(1);
    }

    @Test
    @DisplayName("T11.extra-7 an executor throwing an Error (not an Exception) is still reported as EXECUTOR_CRASHED")
    void errorFromExecutor() throws Exception {
        runner(ScriptedExecutor.succeeding().on("assert", context -> {
            throw new AssertionError("broken invariant");
        }), 1);

        runner.onCommand(command("assert", 5_000));

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1 && runner.inFlight() == 0);
        assertThat(published.peek().errorCode()).isEqualTo("EXECUTOR_CRASHED");
    }

    @Test
    @DisplayName("T11.extra-3 an output over 64 KB is reported as non-retryable OUTPUT_TOO_LARGE")
    void outputGuard() throws Exception {
        runner(ScriptedExecutor.succeeding().on("big", context ->
            TaskResult.success(JSON.createObjectNode().put("blob", "x".repeat(70_000)))), 1);

        runner.onCommand(command("big", 5_000));

        await().atMost(Duration.ofSeconds(5)).until(() -> published.size() == 1);
        assertThat(published.peek().errorCode()).isEqualTo("OUTPUT_TOO_LARGE");
        assertThat(published.peek().retryable()).isFalse();
        assertThat(published.peek().output()).isNull();
    }
}

package com.flowforge.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.executor.DelayTaskExecutor;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.core.testing.Behaviours;
import com.flowforge.core.testing.MutableClock;
import com.flowforge.core.testing.ScriptedExecutor;
import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.BackoffType;
import com.flowforge.core.workflow.Priority;
import com.flowforge.core.workflow.RetryPolicySpec;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** T05.13: every behavioural test runs in both PLATFORM and VIRTUAL mode. */
class LocalWorkflowEngineTest {

    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final RetryPolicySpec NO_RETRY = new RetryPolicySpec(1, BackoffType.FIXED, Duration.ZERO, Duration.ZERO,
        1.0, false);

    private final List<LocalWorkflowEngine> engines = new ArrayList<>();

    @AfterEach
    void closeEngines() {
        engines.forEach(LocalWorkflowEngine::close);
    }

    private LocalWorkflowEngine engine(ExecutionMode mode, int parallelism, TaskExecutor... executors) {
        LocalWorkflowEngine engine = new LocalWorkflowEngine(new TaskExecutorRegistry(List.of(executors)),
            new LocalEngineOptions(mode, parallelism), MutableClock.startingAt2026(), new SplittableRandom(5));
        engines.add(engine);
        return engine;
    }

    private static ExecutionReport finish(ExecutionHandle handle) throws Exception {
        return handle.completion().get(WAIT.toSeconds(), TimeUnit.SECONDS);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.2 linear workflow completes; start order respects dependencies (T05.13: both modes)")
    void linearWorkflowCompletes(ExecutionMode mode) throws Exception {
        ScriptedExecutor executor = ScriptedExecutor.succeeding();
        ExecutionReport report = finish(engine(mode, 4, executor).start(WorkflowFixtures.linear(5), null));

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(executor.startOrder()).containsExactly("t0", "t1", "t2", "t3", "t4");
        assertThat(report.tasks().values()).containsOnly(TaskStatus.SUCCEEDED);
        assertThat(report.outputs().get("t4").get("ok").asBoolean()).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.3 parallelism proof: b, c, d meet at a CyclicBarrier(3); e starts after all three (T05.13: both modes)")
    void diamondRunsConcurrently(ExecutionMode mode) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(3);
        ScriptedExecutor executor = ScriptedExecutor.succeeding()
            .on("b", Behaviours.barrier(barrier)).on("c", Behaviours.barrier(barrier)).on("d", Behaviours.barrier(barrier));

        ExecutionReport report = finish(engine(mode, 4, executor).start(WorkflowFixtures.diamond(), null));

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        long lastMiddleFinish = Math.max(executor.lastFinishNanos("b"),
            Math.max(executor.lastFinishNanos("c"), executor.lastFinishNanos("d")));
        assertThat(executor.firstStartNanos("e")).isGreaterThan(lastMiddleFinish);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.4 20 independent tasks at parallelism 4: high-water mark of concurrent executions is exactly 4 "
        + "(T05.13: both modes)")
    void parallelismIsBounded(ExecutionMode mode) throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, Behaviours.counting(Duration.ofMillis(30)));
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        for (int i = 0; i < 20; i++) {
            builder.task("t" + i, TaskType.DELAY);
        }

        ExecutionReport report = finish(engine(mode, 4, executor).start(builder.build(), null));

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(executor.maxConcurrent()).isEqualTo(4);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.5 priority: with the only thread blocked, LOW, NORMAL, CRITICAL, HIGH run as CRITICAL, HIGH, NORMAL, LOW "
        + "(T05.13: both modes)")
    void priorityOrder(ExecutionMode mode) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        ScriptedExecutor executor = ScriptedExecutor.succeeding().on("blocker", Behaviours.latch(release));
        LocalWorkflowEngine engine = engine(mode, 1, executor);

        ExecutionHandle blocker = engine.start(WorkflowDefinition.builder().task("blocker", TaskType.DELAY).build(), null);
        await().atMost(WAIT).until(() -> blocker.snapshot().tasks().get("blocker") == TaskStatus.RUNNING);

        ExecutionHandle prioritized = engine.start(WorkflowDefinition.builder()
            .task("t1-low", TaskType.DELAY).priority(Priority.LOW)
            .task("t2-normal", TaskType.DELAY).priority(Priority.NORMAL)
            .task("t3-critical", TaskType.DELAY).priority(Priority.CRITICAL)
            .task("t4-high", TaskType.DELAY).priority(Priority.HIGH)
            .build(), null);
        await().atMost(WAIT).until(() -> prioritized.snapshot().tasks().values().stream().allMatch(s -> s == TaskStatus.QUEUED));
        release.countDown();

        assertThat(finish(prioritized).status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(executor.startOrder()).containsExactly("blocker", "t3-critical", "t4-high", "t2-normal", "t1-low");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.6 FlakyExecutor(2) with maxAttempts 3 succeeds on attempt 3; both retry delays match the policy "
        + "(T05.13: both modes)")
    void flakyTaskRetriesThenSucceeds(ExecutionMode mode) throws Exception {
        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, Behaviours.flaky(2, true));
        RetryPolicySpec retry = new RetryPolicySpec(3, BackoffType.LINEAR, Duration.ofMillis(20), Duration.ofMillis(100), 1.0,
            false);

        ExecutionReport report = finish(engine(mode, 2, executor)
            .start(WorkflowDefinition.builder().task("flaky", TaskType.DELAY).retry(retry).build(), null));

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(report.attempts().get("flaky")).isEqualTo(3);
        assertThat(report.outputs().get("flaky").get("attempt").asInt()).isEqualTo(3);
        assertThat(report.retryDelays("flaky")).containsExactly(Duration.ofMillis(20), Duration.ofMillis(40));
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.7 timeout: an attempt past its deadline is interrupted, recorded TIMED_OUT and retried; its late "
        + "completion is ignored (T05.13: both modes)")
    void timeoutInterruptsRetriesAndIgnoresLateResult(ExecutionMode mode) throws Exception {
        AtomicBoolean interrupted = new AtomicBoolean();
        // Attempt 1 ignores the interrupt and finishes late; attempt 2 is quick.
        ScriptedExecutor.Behaviour stuckOnce = context -> context.attempt() == 1
            ? Behaviours.slow(Duration.ofMillis(600), false, interrupted).run(context)
            : Behaviours.succeed().run(context);
        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, stuckOnce);
        RetryPolicySpec retry = new RetryPolicySpec(3, BackoffType.FIXED, Duration.ofMillis(10), Duration.ofMillis(10), 1.0, false);

        ExecutionHandle handle = engine(mode, 2, executor).start(WorkflowDefinition.builder()
            .task("stuck", TaskType.DELAY).timeout(Duration.ofMillis(150)).retry(retry).build(), null);
        ExecutionReport report = finish(handle);

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(report.attempts().get("stuck")).isEqualTo(2);
        assertThat(report.events("stuck", TimelineEvent.Type.TIMED_OUT)).extracting(TimelineEvent::attempt).containsExactly(1);
        assertThat(interrupted).isTrue();

        await().atMost(WAIT).until(() -> !handle.snapshot().events("stuck", TimelineEvent.Type.STALE_RESULT_IGNORED).isEmpty());
        ExecutionReport later = handle.snapshot();
        assertThat(later.events("stuck", TimelineEvent.Type.STALE_RESULT_IGNORED)).extracting(TimelineEvent::attempt)
            .containsExactly(1);
        assertThat(later.tasks().get("stuck")).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(later.outputs().get("stuck").get("ok").asBoolean()).isTrue();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.8 a non-retryable failure fails the workflow and its dependents never start (T05.13: both modes)")
    void nonRetryableFailureFailsWorkflow(ExecutionMode mode) throws Exception {
        ScriptedExecutor executor = ScriptedExecutor.succeeding().on("t0", Behaviours.flaky(1, false));

        ExecutionReport report = finish(engine(mode, 2, executor).start(WorkflowFixtures.linear(3), null));

        assertThat(report.status()).isEqualTo(WorkflowStatus.FAILED);
        assertThat(report.tasks()).containsEntry("t0", TaskStatus.FAILED_PERMANENTLY)
            .containsEntry("t1", TaskStatus.PENDING).containsEntry("t2", TaskStatus.PENDING);
        assertThat(executor.invocations("t1")).isZero();
        assertThat(executor.invocations("t2")).isZero();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.9 cancel during long DELAY tasks is observed within 200 ms; workflow CANCELLED; nothing new starts "
        + "(T05.13: both modes)")
    void cancelStopsRunningTasks(ExecutionMode mode) throws Exception {
        WorkflowDefinition definition = WorkflowDefinition.builder()
            .task("long-a", TaskType.DELAY).config(delay(10_000)).retry(NO_RETRY)
            .task("long-b", TaskType.DELAY).config(delay(10_000)).retry(NO_RETRY)
            .task("after", TaskType.DELAY).config(delay(0)).dependsOn("long-a", "long-b")
            .build();
        ExecutionHandle handle = engine(mode, 4, new DelayTaskExecutor()).start(definition, null);
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("long-a") == TaskStatus.RUNNING
            && handle.snapshot().tasks().get("long-b") == TaskStatus.RUNNING);

        long cancelledAt = System.nanoTime();
        handle.cancel();
        ExecutionReport report = finish(handle);

        assertThat(Duration.ofNanos(System.nanoTime() - cancelledAt)).isLessThan(Duration.ofMillis(200));
        assertThat(report.status()).isEqualTo(WorkflowStatus.CANCELLED);
        assertThat(report.tasks().values()).containsOnly(TaskStatus.CANCELLED);
        assertThat(report.events("after", TimelineEvent.Type.STARTED)).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.10 pause after a: b, c, d don't start while paused; resume → COMPLETED (T05.13: both modes)")
    void pauseAndResume(ExecutionMode mode) throws Exception {
        CountDownLatch releaseA = new CountDownLatch(1);
        ScriptedExecutor executor = ScriptedExecutor.succeeding().on("a", Behaviours.latch(releaseA));
        ExecutionHandle handle = engine(mode, 4, executor).start(WorkflowFixtures.diamond(), null);
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("a") == TaskStatus.RUNNING);

        handle.pause();
        releaseA.countDown();
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("a") == TaskStatus.SUCCEEDED);

        // Hold for 300 ms: while paused, the ready tasks must stay READY and nothing may start.
        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() -> {
            ExecutionReport snapshot = handle.snapshot();
            return snapshot.status() == WorkflowStatus.PAUSED
                && Set.of("b", "c", "d").stream().allMatch(k -> snapshot.tasks().get(k) == TaskStatus.READY)
                && executor.invocations("b") + executor.invocations("c") + executor.invocations("d") == 0;
        });

        handle.resume();
        ExecutionReport report = finish(handle);

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(executor.startOrder()).containsExactlyInAnyOrder("a", "b", "c", "d", "e");
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.9 cancel while the last task runs and ignores its token: the late success is recorded and the "
        + "workflow still ends CANCELLED (T05.13: both modes)")
    void cancelWinsOverLateSuccess(ExecutionMode mode) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        ScriptedExecutor executor = ScriptedExecutor.succeeding().on("only", context -> {
            while (release.getCount() > 0) { // ignores both the token and the interrupt until released
                try {
                    release.await();
                } catch (InterruptedException ignored) {
                    // keep waiting
                }
            }
            return TaskResult.success(JsonNodeFactory.instance.objectNode().put("late", true));
        });
        ExecutionHandle handle = engine(mode, 2, executor).start(WorkflowDefinition.builder().task("only", TaskType.DELAY).build(), null);
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("only") == TaskStatus.RUNNING);

        handle.cancel();
        await().atMost(WAIT).until(() -> handle.snapshot().status() == WorkflowStatus.CANCELLING);
        release.countDown();
        ExecutionReport report = finish(handle);

        assertThat(report.status()).isEqualTo(WorkflowStatus.CANCELLED);
        assertThat(report.tasks().get("only")).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.10 a task already QUEUED when the pause lands does not start until resume (T05.13: both modes)")
    void queuedTaskWaitsForResume(ExecutionMode mode) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        ScriptedExecutor executor = ScriptedExecutor.succeeding().on("blocker", Behaviours.latch(release));
        ExecutionHandle handle = engine(mode, 1, executor).start(WorkflowDefinition.builder()
            .task("blocker", TaskType.DELAY).task("x1", TaskType.DELAY).task("x2", TaskType.DELAY).build(), null);
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("blocker") == TaskStatus.RUNNING
            && handle.snapshot().tasks().get("x1") == TaskStatus.QUEUED);

        handle.pause();
        await().atMost(WAIT).until(() -> handle.snapshot().status() == WorkflowStatus.PAUSED);
        release.countDown();

        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() ->
            executor.invocations("x1") + executor.invocations("x2") == 0
                && handle.snapshot().tasks().get("x1") == TaskStatus.QUEUED);

        handle.resume();
        assertThat(finish(handle).status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(executor.invocations("x1")).isEqualTo(1);
        assertThat(executor.invocations("x2")).isEqualTo(1);
    }

    @RepeatedTest(50)
    @DisplayName("T05.10 rapid pause/resume never strands a queued task (claim rejected after the resume); "
        + "alternates PLATFORM and VIRTUAL")
    void rapidPauseResumeCompletes(RepetitionInfo repetition) throws Exception {
        ExecutionMode mode = repetition.getCurrentRepetition() % 2 == 0 ? ExecutionMode.PLATFORM : ExecutionMode.VIRTUAL;
        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, Behaviours.counting(Duration.ofMillis(1)));
        WorkflowDefinition.Builder builder = WorkflowDefinition.builder();
        for (int i = 0; i < 20; i++) {
            builder.task("t" + i, TaskType.DELAY);
        }
        ExecutionHandle handle = engine(mode, 2, executor).start(builder.build(), null);

        // One caller on purpose: the race is between this thread's commands and the workers' claims, so no start gate.
        for (int i = 0; i < 30; i++) {
            handle.pause();
            handle.resume();
        }

        assertThat(finish(handle).status()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @ParameterizedTest(name = "{0}")
    @EnumSource(ExecutionMode.class)
    @DisplayName("T05.extra-5 a quiet execution's snapshot is published even when its last event was followed by "
        + "another execution's events (T05.13: both modes)")
    void snapshotStaysCurrentUnderLoad(ExecutionMode mode) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, Behaviours.latch(release));
        LocalWorkflowEngine engine = engine(mode, 4, executor);
        ExecutionHandle quiet = engine.start(WorkflowDefinition.builder().task("q", TaskType.DELAY).build(), null);
        ExecutionHandle busy = engine.start(WorkflowDefinition.builder().task("b", TaskType.DELAY).build(), null);
        await().atMost(WAIT).until(() -> quiet.snapshot().tasks().get("q") == TaskStatus.RUNNING
            && busy.snapshot().tasks().get("b") == TaskStatus.RUNNING);

        // Each command for the quiet execution is immediately followed by events for the busy one (no-op RESUMEs of a
        // RUNNING execution), so the queue is not empty when the quiet one's change is processed.
        for (int i = 0; i < 20; i++) {
            boolean pause = i % 2 == 0;
            engine.post(new EngineEvent.Command(quiet.id(),
                pause ? EngineEvent.Command.Kind.PAUSE : EngineEvent.Command.Kind.RESUME));
            for (int n = 0; n < 5; n++) {
                engine.post(new EngineEvent.Command(busy.id(), EngineEvent.Command.Kind.RESUME));
            }
            WorkflowStatus expected = pause ? WorkflowStatus.PAUSED : WorkflowStatus.RUNNING;
            await().atMost(Duration.ofSeconds(2)).until(() -> quiet.snapshot().status() == expected);
        }

        release.countDown();
        assertThat(finish(quiet).status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(finish(busy).status()).isEqualTo(WorkflowStatus.COMPLETED);
    }

    @RepeatedTest(50)
    @DisplayName("T05.7 a timeout interrupt never leaks to the next task on the same pool thread")
    void timeoutInterruptDoesNotLeak() throws Exception {
        AtomicBoolean nextSawInterrupt = new AtomicBoolean();
        ScriptedExecutor executor = ScriptedExecutor.succeeding()
            .on("a", context -> {
                if (context.attempt() == 1) {
                    try {
                        Thread.sleep(5_000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt(); // re-assert, as careless code does, then return late
                    }
                }
                return TaskResult.success(JsonNodeFactory.instance.objectNode());
            })
            .on("b", context -> {
                nextSawInterrupt.set(Thread.currentThread().isInterrupted());
                return TaskResult.success(JsonNodeFactory.instance.objectNode());
            });
        RetryPolicySpec retry = new RetryPolicySpec(2, BackoffType.FIXED, Duration.ZERO, Duration.ZERO, 1.0, false);

        ExecutionReport report = finish(engine(ExecutionMode.PLATFORM, 1, executor).start(WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).timeout(Duration.ofMillis(100)).retry(retry)
            .task("b", TaskType.DELAY).build(), null));

        assertThat(report.status()).isEqualTo(WorkflowStatus.COMPLETED);
        assertThat(nextSawInterrupt).isFalse();
    }

    @Test
    @DisplayName("T05.12 close() interrupts in-flight work and leaves no engine threads behind")
    void closeLeavesNoThreads() {
        AtomicBoolean interrupted = new AtomicBoolean();
        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, Behaviours.slow(Duration.ofSeconds(30), true, interrupted));
        LocalWorkflowEngine engine = engine(ExecutionMode.PLATFORM, 3, executor);
        ExecutionHandle handle = engine.start(WorkflowFixtures.fanOut(2), null);
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("root") == TaskStatus.RUNNING);

        engine.close();

        assertThat(interrupted).isTrue();
        assertThat(handle.completion()).isCompletedExceptionally();
        await().atMost(Duration.ofSeconds(5)).until(() -> liveEngineThreads().isEmpty());
    }

    @Test
    @DisplayName("T05.12 close() in VIRTUAL mode also stops the dispatcher, coordinator and timer threads")
    void closeLeavesNoThreadsVirtual() {
        LocalWorkflowEngine engine = engine(ExecutionMode.VIRTUAL, 3, new DelayTaskExecutor());
        ExecutionHandle handle = engine.start(WorkflowDefinition.builder().task("wait", TaskType.DELAY).config(delay(30_000)).build(),
            null);
        await().atMost(WAIT).until(() -> handle.snapshot().tasks().get("wait") == TaskStatus.RUNNING);

        engine.close();

        assertThat(handle.completion()).isCompletedExceptionally();
        // Virtual threads are invisible to Thread.getAllStackTraces(), so ask the strategy how many workers remain.
        assertThat(engine.activeWorkers()).isZero();
        await().atMost(Duration.ofSeconds(5)).until(() -> liveEngineThreads().isEmpty());
    }

    private static Set<String> liveEngineThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(Thread::isAlive)
            .map(Thread::getName)
            .filter(name -> name.startsWith(LocalWorkflowEngine.THREAD_PREFIX))
            .collect(Collectors.toSet());
    }

    private static JsonNode delay(long millis) {
        return JsonNodeFactory.instance.objectNode().put("durationMs", millis);
    }
}

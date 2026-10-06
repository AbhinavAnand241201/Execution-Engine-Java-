package com.flowforge.core.engine;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.dag.Dag;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.core.testing.MutableClock;
import com.flowforge.core.testing.ScriptedExecutor;
import com.flowforge.core.testing.WorkflowFixtures;
import com.flowforge.core.workflow.BackoffType;
import com.flowforge.core.workflow.RetryPolicySpec;
import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;

/**
 * T05.11. Each of 67 repetitions runs three random 50-task workflows at once on one engine (201 DAGs in total,
 * released together by a start gate), with random latency, retryable and fatal failures, attempts that time out, and one workflow cancelled
 * mid-flight. The invariants are checked from the executor's side, independently of the engine's own report.
 */
class LocalEngineStressTest {

    private static final int EXECUTIONS = 3;
    private static final RetryPolicySpec FAST_RETRY = new RetryPolicySpec(3, BackoffType.FIXED, Duration.ofMillis(1),
        Duration.ofMillis(1), 1.0, false);

    @RepeatedTest(67)
    @DisplayName("T05.11 concurrent random DAGs with latency, failures, timeouts and a cancel: invariants hold")
    void randomDagInvariants(RepetitionInfo repetition) throws Exception {
        long seed = 1_000L + repetition.getCurrentRepetition();
        ExecutionMode mode = repetition.getCurrentRepetition() % 2 == 0 ? ExecutionMode.PLATFORM : ExecutionMode.VIRTUAL;
        Random random = new Random(seed);

        List<WorkflowDefinition> definitions = new ArrayList<>();
        Map<String, Integer> plannedFailures = new ConcurrentHashMap<>();
        Set<String> plannedFatal = ConcurrentHashMap.newKeySet();
        Set<String> plannedTimeout = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < EXECUTIONS; i++) {
            WorkflowDefinition random50 = WorkflowFixtures.randomDag(seed * 31 + i, 50);
            List<TaskDefinition> tasks = new ArrayList<>();
            for (TaskDefinition t : random50.tasks()) {
                boolean timesOut = random.nextInt(20) == 0;
                if (timesOut) {
                    plannedTimeout.add(t.key());
                }
                if (random.nextInt(10) == 0) {
                    plannedFailures.put(t.key(), 1 + random.nextInt(2));
                }
                if (random.nextInt(100) == 0) {
                    plannedFatal.add(t.key());
                }
                tasks.add(new TaskDefinition(t.key(), t.type(), t.config(), t.dependsOn(),
                    timesOut ? Duration.ofMillis(100) : t.timeout(), t.priority(), FAST_RETRY));
            }
            definitions.add(new WorkflowDefinition(tasks));
        }

        // Keyed by execution id + task key: the three workflows reuse the same task keys.
        Set<String> succeeded = ConcurrentHashMap.newKeySet();
        Map<String, AtomicInteger> running = new ConcurrentHashMap<>();
        Queue<String> violations = new ConcurrentLinkedQueue<>();
        Map<UUID, Dag> dags = new ConcurrentHashMap<>();

        ScriptedExecutor executor = new ScriptedExecutor(TaskType.DELAY, context -> {
            String key = context.taskKey();
            String id = context.workflowExecutionId() + "/" + key;
            // A timed-out attempt may still be running ("zombie") when its retry starts; that overlap is by design and
            // fenced by attempt number, so the no-overlap invariant is only checked for tasks that never time out.
            boolean checkOverlap = !plannedTimeout.contains(key);
            if (checkOverlap && running.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet() > 1) {
                violations.add(id + " ran twice concurrently");
            }
            try {
                Dag dag;
                while ((dag = dags.get(context.workflowExecutionId())) == null) {
                    Thread.onSpinWait(); // the starter thread registers the DAG right after start() returns
                }
                for (String dependency : dag.dependencies(key)) {
                    if (!succeeded.contains(context.workflowExecutionId() + "/" + dependency)) {
                        violations.add(id + " started before " + dependency + " succeeded");
                    }
                }
                if (plannedTimeout.contains(key) && context.attempt() == 1) {
                    Thread.sleep(150); // past its 100 ms timeout; the interrupt ends it early
                }
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(ThreadLocalRandom.current().nextLong(0, 2_000)));
                if (plannedFatal.contains(key)) {
                    return TaskResult.failure("FATAL", "planned fatal failure", false);
                }
                if (context.attempt() <= plannedFailures.getOrDefault(key, 0)) {
                    return TaskResult.failure("FLAKY", "planned retryable failure", true);
                }
                succeeded.add(id);
                return TaskResult.success(JsonNodeFactory.instance.objectNode().put("ok", true));
            } finally {
                if (checkOverlap) {
                    running.get(id).decrementAndGet();
                }
            }
        });

        try (LocalWorkflowEngine engine = new LocalWorkflowEngine(new TaskExecutorRegistry(List.of(executor)),
            new LocalEngineOptions(mode, 8), MutableClock.startingAt2026(), new SplittableRandom(seed))) {
            CountDownLatch startGate = new CountDownLatch(1);
            List<ExecutionHandle> handles = new ArrayList<>(Collections.nCopies(EXECUTIONS, null));
            List<Thread> starters = new ArrayList<>();
            for (int i = 0; i < EXECUTIONS; i++) {
                int index = i;
                Thread starter = new Thread(() -> {
                    try {
                        startGate.await();
                    } catch (InterruptedException e) {
                        return;
                    }
                    ExecutionHandle handle = engine.start(definitions.get(index), null);
                    dags.put(handle.id(), Dag.of(definitions.get(index)));
                    synchronized (handles) {
                        handles.set(index, handle);
                    }
                    if (index == 0) {
                        // Cancel the first workflow a moment into its run.
                        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(random.nextInt(3_000)));
                        handle.cancel();
                    }
                });
                starters.add(starter);
                starter.start();
            }
            startGate.countDown();
            for (Thread starter : starters) {
                starter.join();
            }

            for (int i = 0; i < EXECUTIONS; i++) {
                ExecutionReport report = handles.get(i).completion().get(30, TimeUnit.SECONDS);
                assertThat(report.tasks().values())
                    .as("execution %d", i).doesNotContain(TaskStatus.RUNNING, TaskStatus.RETRYING);
                switch (report.status()) {
                    case COMPLETED -> assertThat(report.tasks().values()).containsOnly(TaskStatus.SUCCEEDED);
                    // Claims are refused once the workflow failed, so already-queued tasks may stay QUEUED.
                    case FAILED -> assertThat(report.tasks().values()).contains(TaskStatus.FAILED_PERMANENTLY);
                    case CANCELLED -> assertThat(report.tasks().values())
                        .as("execution %d", i).isSubsetOf(TaskStatus.SUCCEEDED, TaskStatus.CANCELLED, TaskStatus.FAILED_PERMANENTLY);
                    default -> throw new AssertionError("unexpected final status " + report.status());
                }
                if (i > 0) {
                    assertThat(report.status()).isNotEqualTo(WorkflowStatus.CANCELLED);
                }
                for (Map.Entry<String, TaskStatus> task : report.tasks().entrySet()) {
                    if (task.getValue() == TaskStatus.SUCCEEDED) {
                        assertThat(succeeded).contains(handles.get(i).id() + "/" + task.getKey());
                    }
                }
            }
            assertThat(violations).isEmpty();
        }
    }
}

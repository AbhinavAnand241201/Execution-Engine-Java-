package com.flowforge.core.testing;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.executor.TaskResult;
import java.time.Duration;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test executors as behaviours for {@link ScriptedExecutor}. Some simulate work by sleeping; that
 * is the work under test, not a test waiting for an outcome (which always uses Awaitility).
 */
public final class Behaviours {

    private Behaviours() {
    }

    /** Succeeds at once with {@code {"ok": true}}. */
    public static ScriptedExecutor.Behaviour succeed() {
        return context -> TaskResult.success(JsonNodeFactory.instance.objectNode().put("ok", true));
    }

    /** "CountingExecutor": succeeds after {@code work} of simulated work; invocations are counted by the executor. */
    public static ScriptedExecutor.Behaviour counting(Duration work) {
        return context -> {
            Thread.sleep(work.toMillis());
            return TaskResult.success(JsonNodeFactory.instance.objectNode().put("ok", true));
        };
    }

    /** "LatchExecutor": blocks until {@code release} is counted down, then succeeds. */
    public static ScriptedExecutor.Behaviour latch(CountDownLatch release) {
        return context -> {
            release.await();
            return TaskResult.success(JsonNodeFactory.instance.objectNode().put("released", true));
        };
    }

    /** "FlakyExecutor": fails the first {@code failTimes} attempts, then succeeds. */
    public static ScriptedExecutor.Behaviour flaky(int failTimes, boolean retryable) {
        return context -> context.attempt() <= failTimes
            ? TaskResult.failure("FLAKY", "planned failure on attempt " + context.attempt(), retryable)
            : TaskResult.success(JsonNodeFactory.instance.objectNode().put("attempt", context.attempt()));
    }

    /**
     * "SlowExecutor": runs for {@code duration}. If {@code honoursInterrupt}, an interrupt ends it early (and is
     * recorded in {@code interrupted}); otherwise it keeps going and finishes late, like a stuck call.
     */
    public static ScriptedExecutor.Behaviour slow(Duration duration, boolean honoursInterrupt, AtomicBoolean interrupted) {
        return context -> {
            long end = System.nanoTime() + duration.toNanos();
            while (System.nanoTime() < end) {
                try {
                    Thread.sleep(Math.max(1, TimeUnit.NANOSECONDS.toMillis(end - System.nanoTime())));
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    if (honoursInterrupt) {
                        throw e;
                    }
                }
            }
            return TaskResult.success(JsonNodeFactory.instance.objectNode().put("slow", true));
        };
    }

    /** "BarrierExecutor": waits at {@code barrier} (2 s timeout); only succeeds if all parties arrive together. */
    public static ScriptedExecutor.Behaviour barrier(CyclicBarrier barrier) {
        return context -> {
            try {
                barrier.await(2, TimeUnit.SECONDS);
                return TaskResult.success(JsonNodeFactory.instance.objectNode().put("met", true));
            } catch (BrokenBarrierException | TimeoutException e) {
                return TaskResult.failure("BARRIER_BROKEN", "tasks did not run concurrently", false);
            }
        };
    }
}

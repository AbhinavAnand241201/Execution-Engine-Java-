package com.flowforge.core.testing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * Runs the same action on several threads released together by a start gate, many times over. Tests then assert
 * invariants on the results ("exactly one winner"), never on timing.
 */
public final class ConcurrencyHarness {

    private ConcurrencyHarness() {
    }

    /**
     * @param action receives the thread index (0..threads-1) and returns that thread's outcome; an exception thrown by
     *               the action becomes the outcome, so tests can assert on failures too
     * @return one list of per-thread outcomes for every iteration
     */
    public static List<List<Object>> race(int threads, int iterations, IntFunction<Object> action) throws Exception {
        return race(threads, iterations, action, () -> { });
    }

    /** As {@link #race(int, int, IntFunction)}, running {@code beforeEachIteration} (e.g. new test data) first. */
    public static List<List<Object>> race(int threads, int iterations, IntFunction<Object> action,
                                          Runnable beforeEachIteration) throws Exception {
        List<List<Object>> all = new ArrayList<>();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int iteration = 0; iteration < iterations; iteration++) {
                beforeEachIteration.run();
                CountDownLatch ready = new CountDownLatch(threads);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Object>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    int index = t;
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        go.await();
                        try {
                            return action.apply(index);
                        } catch (RuntimeException e) {
                            return e;
                        }
                    }));
                }
                if (!ready.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("not all " + threads + " threads reached the start gate");
                }
                go.countDown();
                List<Object> outcomes = new ArrayList<>();
                for (Future<Object> future : futures) {
                    outcomes.add(future.get(30, TimeUnit.SECONDS));
                }
                all.add(outcomes);
            }
        } finally {
            pool.shutdownNow();
        }
        return all;
    }
}

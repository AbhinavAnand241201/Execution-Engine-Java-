package com.flowforge.core.engine;

import java.time.Duration;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A fixed pool of {@code parallelism} platform threads whose work queue is a {@link PriorityBlockingQueue}, so when
 * all threads are busy the most urgent waiting task runs next. Tasks go in with {@code execute()} (see
 * {@link PrioritizedTask} for why not {@code submit()}).
 */
final class PlatformStrategy implements ExecutionStrategy {

    private final ThreadPoolExecutor pool;

    PlatformStrategy(int parallelism) {
        AtomicInteger counter = new AtomicInteger();
        this.pool = new ThreadPoolExecutor(parallelism, parallelism, 0L, TimeUnit.MILLISECONDS,
            new PriorityBlockingQueue<>(), runnable -> {
                Thread thread = new Thread(runnable, LocalWorkflowEngine.THREAD_PREFIX + "worker-" + counter.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
    }

    @Override
    public void submit(PrioritizedTask task) {
        pool.execute(task);
    }

    @Override
    public int activeWorkers() {
        return pool.getActiveCount();
    }

    @Override
    public void shutdown(Duration grace) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(grace.toMillis(), TimeUnit.MILLISECONDS)) {
                pool.shutdownNow(); // interrupts running tasks; queued ones are dropped
                pool.awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

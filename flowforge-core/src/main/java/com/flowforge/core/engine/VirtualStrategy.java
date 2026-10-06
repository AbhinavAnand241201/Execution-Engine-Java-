package com.flowforge.core.engine;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * One virtual thread per task. Virtual threads are cheap, so there is no pool; instead a dispatcher thread takes a
 * permit from a {@code Semaphore(parallelism)} <em>before</em> taking the most urgent task from a priority queue.
 * Waiting for the permit first means the choice of task is made at the last moment, so priority still decides
 * which task runs next when every slot is busy.
 */
final class VirtualStrategy implements ExecutionStrategy {

    private final PriorityBlockingQueue<PrioritizedTask> queue = new PriorityBlockingQueue<>();
    private final Semaphore permits;
    private final Set<Thread> running = ConcurrentHashMap.newKeySet();
    private final Thread dispatcher;
    private volatile boolean stopped;

    VirtualStrategy(int parallelism) {
        this.permits = new Semaphore(parallelism);
        this.dispatcher = new Thread(this::dispatchLoop, LocalWorkflowEngine.THREAD_PREFIX + "dispatcher");
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    @Override
    public void submit(PrioritizedTask task) {
        queue.add(task);
    }

    private void dispatchLoop() {
        while (!stopped) {
            try {
                permits.acquire();
            } catch (InterruptedException e) {
                return;
            }
            PrioritizedTask task;
            try {
                task = queue.take();
            } catch (InterruptedException e) {
                permits.release();
                return;
            }
            Thread worker = Thread.ofVirtual().name(LocalWorkflowEngine.THREAD_PREFIX + "vt").unstarted(() -> {
                try {
                    task.run();
                } finally {
                    running.remove(Thread.currentThread());
                    permits.release();
                }
            });
            running.add(worker);
            worker.start();
        }
    }

    @Override
    public int activeWorkers() {
        return running.size();
    }

    @Override
    public void shutdown(Duration grace) {
        stopped = true;
        dispatcher.interrupt();
        try {
            dispatcher.join(TimeUnit.SECONDS.toMillis(5));
            long deadline = System.nanoTime() + grace.toNanos();
            for (Thread worker : Set.copyOf(running)) {
                worker.join(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())));
            }
            for (Thread worker : Set.copyOf(running)) {
                worker.interrupt();
                worker.join(TimeUnit.SECONDS.toMillis(5));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.flowforge.orchestrator.engine;

import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.runtime.runner.CommandOutcome;
import com.flowforge.runtime.runner.TaskRunner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Local mode: hands commands to the in-process {@link TaskRunner}, only after the dispatch transaction commits.
 * Before the commit the task is still READY to everyone else, so the runner's claim (QUEUED → RUNNING) would fail.
 * On RETRY_LATER (no free permit) it tries again after 200 ms. Delivery runs on its own threads because
 * {@code onCommand} may wait for a permit.
 */
public class LocalTaskDispatcher implements TaskDispatcher, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LocalTaskDispatcher.class);
    static final long RETRY_LATER_MS = 200;

    private final TaskRunner runner;
    /** (task, attempt) of commands accepted for delivery and not finished with yet (claimed, dropped or failed). */
    private final Set<String> pending = ConcurrentHashMap.newKeySet();
    private final ExecutorService delivery = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("ff-local-dispatch-", 0).factory());
    private final ScheduledExecutorService retries = Executors.newSingleThreadScheduledExecutor(
        Thread.ofPlatform().daemon().name("ff-local-retry").factory());

    public LocalTaskDispatcher(TaskRunner runner) {
        this.runner = runner;
    }

    @Override
    public void dispatch(TaskCommand command) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            throw new IllegalStateException("dispatch must run inside a transaction (delivery happens after commit)");
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                // A re-send (QueueReconciler) of a command still waiting here for a permit would only start a
                // second retry loop for the same attempt: drop it.
                if (pending.add(key(command))) {
                    delivery.execute(() -> deliver(command));
                }
            }
        });
    }

    private void deliver(TaskCommand command) {
        boolean done = true;
        try {
            if (runner.onCommand(command) == CommandOutcome.RETRY_LATER) {
                done = false;
                retries.schedule(() -> delivery.execute(() -> deliver(command)), RETRY_LATER_MS, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // The task stays QUEUED; the QueueReconciler re-sends it.
            log.warn("local delivery failed taskExecutionId={} error={}", command.taskExecutionId(), e.toString());
        } finally {
            if (done) {
                pending.remove(key(command));
            }
        }
    }

    private static String key(TaskCommand command) {
        return command.taskExecutionId() + ":" + command.attempt();
    }

    @Override
    public void close() {
        retries.shutdownNow();
        delivery.shutdownNow();
    }
}

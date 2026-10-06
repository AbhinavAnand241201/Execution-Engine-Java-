package com.flowforge.runtime.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.executor.CancellationToken;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.runtime.claim.TaskClaimRepository;
import com.flowforge.runtime.claim.TaskInputRepository;
import com.flowforge.runtime.executor.OutputGuard;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Turns a {@link TaskCommand} into one executed attempt, shared by local mode and the worker.
 *
 * <p>Order matters: <b>capacity first, then claim.</b> Claiming without a free permit would mark the task RUNNING on
 * this worker while nothing runs it; its deadline would tick down and it would time out for no reason. With a permit
 * in hand, a won claim always starts immediately; with none, the command is handed back untouched
 * ({@link CommandOutcome#RETRY_LATER}) for another delivery or another worker.
 *
 * <p>Timeouts: a scheduler fires at the attempt's timeout, cancels the token and interrupts the attempt's thread
 * (what {@code future.cancel(true)} would do, but only while the executor is still running); the attempt then reports
 * TIMED_OUT, whatever the executor returned. The permit is
 * released when the attempt's thread is done, so an executor that ignores interrupts keeps its permit (capacity is
 * never over-committed); the orchestrator's timeout sweeper recovers the task either way.
 */
public class TaskRunner implements AutoCloseable {

    /** After a shutdown interrupts attempts: how long they get to report WORKER_SHUTDOWN. */
    private static final java.time.Duration REPORT_WINDOW = java.time.Duration.ofSeconds(5);

    private static final Logger log = LoggerFactory.getLogger(TaskRunner.class);

    private final TaskRunnerSettings settings;
    private final TaskExecutorRegistry executors;
    private final TaskClaimRepository claims;
    private final TaskInputRepository inputs;
    private final TaskResultSink sink;
    private final OutputGuard outputGuard;
    private final Clock clock;
    private final Semaphore capacity;
    private final ExecutorService pool;
    private final ScheduledExecutorService timeouts;
    private final AtomicLong duplicateOrStale = new AtomicLong();
    private final AtomicLong publishFailures = new AtomicLong();
    private volatile boolean closed;

    public TaskRunner(TaskRunnerSettings settings, TaskExecutorRegistry executors, TaskClaimRepository claims,
                      TaskInputRepository inputs, TaskResultSink sink, OutputGuard outputGuard, Clock clock) {
        this.settings = settings;
        this.executors = executors;
        this.claims = claims;
        this.inputs = inputs;
        this.sink = sink;
        this.outputGuard = outputGuard;
        this.clock = clock;
        this.capacity = new Semaphore(settings.maxConcurrency());
        this.pool = settings.threadMode() == ThreadMode.VIRTUAL
            ? Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("ff-task-", 0).factory())
            : platformPool(settings.maxConcurrency());
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("ff-task-timeouts").factory());
        // A finished attempt cancels its timeout; drop it from the queue at once instead of keeping it until it fires.
        scheduler.setRemoveOnCancelPolicy(true);
        this.timeouts = scheduler;
    }

    /**
     * One named thread per permit. The queue is unbounded on purpose: the semaphore already caps submissions, and a
     * permit is released just before its thread returns to the pool, so a bounded queue could reject a legal task.
     */
    private static ExecutorService platformPool(int threads) {
        AtomicInteger number = new AtomicInteger();
        return new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(),
            runnable -> Thread.ofPlatform().name("ff-task-" + number.getAndIncrement()).unstarted(runnable));
    }

    public CommandOutcome onCommand(TaskCommand command) throws InterruptedException {
        if (closed || !capacity.tryAcquire(settings.acquireTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
            return CommandOutcome.RETRY_LATER;
        }
        boolean started = false;
        try {
            if (closed) {
                // Shutting down: don't claim work this process can no longer run; another delivery will pick it up.
                return CommandOutcome.RETRY_LATER;
            }
            Instant claimedAt = clock.instant();
            if (!claims.claim(command.taskExecutionId(), command.attempt(), settings.workerId(), claimedAt)) {
                // Duplicate delivery, stale attempt, or the execution is paused/cancelling: nothing to run.
                duplicateOrStale.incrementAndGet();
                log.debug("claim rejected taskExecutionId={} attempt={}", command.taskExecutionId(), command.attempt());
                return CommandOutcome.ACK;
            }
            start(command, claimedAt);
            started = true;
            return CommandOutcome.ACK;
        } finally {
            if (!started) {
                capacity.release();
            }
        }
    }

    /**
     * Schedules the timeout first, then submits; after {@code pool.execute} returns nothing else can fail, so the
     * permit is released exactly once: by the attempt's thread, or by {@link #onCommand} if submission was refused
     * (a shutdown race; the claimed task is then recovered by the timeout sweeper).
     */
    private void start(TaskCommand command, Instant claimedAt) {
        Attempt attempt = new Attempt();
        attempt.timeout = timeouts.schedule(attempt::timeOut, command.timeoutMs(), TimeUnit.MILLISECONDS);
        try {
            pool.execute(() -> {
                try {
                    withMdc(command, () -> run(command, claimedAt, attempt));
                } finally {
                    capacity.release();
                }
            });
        } catch (RuntimeException rejected) {
            attempt.timeout.cancel(false);
            throw rejected;
        }
    }

    /**
     * One attempt's race between "the executor returned" and "the timeout fired". Whoever moves the state first wins,
     * under a lock, so the timeout can only interrupt the thread while the executor is still running, never later
     * while the result is being published.
     */
    private static final class Attempt {
        private enum State { WAITING, RUNNING, FINISHED, TIMED_OUT }

        final CancellationToken token = new CancellationToken();
        private State state = State.WAITING;
        private Thread thread;
        volatile ScheduledFuture<?> timeout;

        /** False if the timeout fired while the attempt was still waiting for a thread: don't run it at all. */
        synchronized boolean started() {
            if (state == State.TIMED_OUT) {
                return false;
            }
            state = State.RUNNING;
            thread = Thread.currentThread();
            return true;
        }

        synchronized void timeOut() {
            if (state == State.WAITING || state == State.RUNNING) {
                if (state == State.RUNNING) {
                    thread.interrupt();
                }
                state = State.TIMED_OUT;
                token.cancel();
            }
        }

        /** True if the executor finished first; false if the attempt had already timed out. */
        synchronized boolean finish() {
            if (state == State.RUNNING) {
                state = State.FINISHED;
            }
            timeout.cancel(false);
            return state == State.FINISHED;
        }
    }

    private void run(TaskCommand command, Instant claimedAt, Attempt attempt) {
        TaskResult result = null;
        Throwable fatal = null;
        if (attempt.started()) {
            try {
                Map<String, JsonNode> upstream = inputs.load(command);
                TaskContext context = new TaskContext(command.workflowExecutionId(), command.taskExecutionId(),
                    command.taskKey(), command.attempt(), command.config(), upstream,
                    claimedAt.plusMillis(command.timeoutMs()), attempt.token, command.correlationId());
                result = outputGuard.check(executors.get(command.type()).execute(context));
            } catch (InterruptedException e) {
                result = null;   // a timeout or a shutdown interrupted it; decided below
            } catch (Exception | Error e) {
                log.warn("executor crashed taskExecutionId={} attempt={} error={}", command.taskExecutionId(),
                    command.attempt(), e.getClass().getSimpleName());
                result = crashed(e);
                fatal = e instanceof VirtualMachineError ? e : null;
            }
        }
        boolean inTime = attempt.finish();
        // The timeout can no longer interrupt (the state left RUNNING under the lock), so any interrupt flag now is
        // from the timeout or from shutdown; clear it so publishing and its backoff aren't cut short.
        Thread.interrupted();
        TaskResultMessage message;
        if (!inTime) {
            message = timedOutMessage(command, claimedAt);
        } else if (result == null) {
            // Interrupted without a timeout: the runner is shutting down. Always retryable, not the task's fault.
            message = outcomeMessage(command, claimedAt, AttemptOutcome.WORKER_SHUTDOWN, "WORKER_SHUTDOWN",
                "worker shut down during the attempt");
        } else {
            message = message(command, claimedAt, result);
        }
        publish(message);
        if (fatal instanceof VirtualMachineError error) {
            throw error;   // out of memory and similar: reported, now let the thread die loudly
        }
    }

    private static TaskResult crashed(Throwable e) {
        return TaskResult.failure("EXECUTOR_CRASHED", "executor threw " + e.getClass().getSimpleName(), true);
    }

    private TaskResultMessage timedOutMessage(TaskCommand command, Instant startedAt) {
        return outcomeMessage(command, startedAt, AttemptOutcome.TIMED_OUT, "TIMEOUT",
            "attempt exceeded " + command.timeoutMs() + " ms");
    }

    /** TIMED_OUT and WORKER_SHUTDOWN: no output, always retryable. */
    private TaskResultMessage outcomeMessage(TaskCommand command, Instant startedAt, AttemptOutcome outcome,
                                             String errorCode, String errorMessage) {
        return new TaskResultMessage(UUID.randomUUID(), TaskResultMessage.SCHEMA_VERSION, command.taskExecutionId(),
            command.workflowExecutionId(), command.attempt(), settings.workerId(), outcome, null, errorCode, errorMessage,
            true, startedAt, clock.instant(), command.correlationId());
    }

    private TaskResultMessage message(TaskCommand command, Instant startedAt, TaskResult result) {
        AttemptOutcome outcome;
        JsonNode output = null;
        String errorCode = null;
        String errorMessage = null;
        boolean retryable = false;
        switch (result) {
            case TaskResult.Success success -> {
                outcome = AttemptOutcome.SUCCEEDED;
                output = success.output();
            }
            case TaskResult.Failure failure -> {
                outcome = AttemptOutcome.FAILED;
                errorCode = failure.errorCode();
                errorMessage = failure.message();
                retryable = failure.retryable();
            }
            case TaskResult.Cancelled cancelled -> outcome = AttemptOutcome.CANCELLED;
        }
        return new TaskResultMessage(UUID.randomUUID(), TaskResultMessage.SCHEMA_VERSION, command.taskExecutionId(),
            command.workflowExecutionId(), command.attempt(), settings.workerId(), outcome, output, errorCode,
            errorMessage, retryable, startedAt, clock.instant(), command.correlationId());
    }

    /** Retries with doubling backoff; if every try fails the result is dropped and the task is recovered by timeout. */
    private void publish(TaskResultMessage message) {
        long backoff = settings.publishBackoff().toMillis();
        for (int attempt = 1; attempt <= settings.publishAttempts(); attempt++) {
            try {
                sink.publish(message);
                return;
            } catch (Exception e) {
                log.warn("result publish failed taskExecutionId={} try={}/{} error={}", message.taskExecutionId(),
                    attempt, settings.publishAttempts(), e.getClass().getSimpleName());
                if (attempt < settings.publishAttempts()) {
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    backoff *= 2;
                }
            }
        }
        publishFailures.incrementAndGet();
        log.error("result dropped after {} tries taskExecutionId={} attempt={}", settings.publishAttempts(),
            message.taskExecutionId(), message.attempt());
    }

    private void withMdc(TaskCommand command, Runnable body) {
        MDC.put("correlationId", command.correlationId());
        MDC.put("workflowExecutionId", String.valueOf(command.workflowExecutionId()));
        MDC.put("taskExecutionId", String.valueOf(command.taskExecutionId()));
        MDC.put("attempt", String.valueOf(command.attempt()));
        MDC.put("workerId", settings.workerId());
        try {
            body.run();
        } finally {
            MDC.clear();
        }
    }

    /** Attempts currently holding a permit (reported in heartbeats). */
    public int inFlight() {
        return settings.maxConcurrency() - capacity.availablePermits();
    }

    public long duplicateOrStaleClaims() {
        return duplicateOrStale.get();
    }

    public long droppedResults() {
        return publishFailures.get();
    }

    /**
     * Stops taking work (new commands get RETRY_LATER); running attempts get {@code grace} to finish, then are
     * interrupted and reported as WORKER_SHUTDOWN (retryable, so a deploy never costs a task its attempts).
     */
    public boolean shutdown(java.time.Duration grace) throws InterruptedException {
        closed = true;
        pool.shutdown();
        boolean finished = pool.awaitTermination(grace.toMillis(), TimeUnit.MILLISECONDS);
        if (!finished) {
            pool.shutdownNow();
            // Interrupted attempts now report WORKER_SHUTDOWN; give them a bounded moment to get that out before the
            // caller tears down what they publish through. An executor that ignores interrupts can't hold us longer.
            pool.awaitTermination(REPORT_WINDOW.toMillis(), TimeUnit.MILLISECONDS);
        }
        timeouts.shutdownNow();
        return finished;
    }

    @Override
    public void close() throws InterruptedException {
        shutdown(java.time.Duration.ZERO);
    }
}

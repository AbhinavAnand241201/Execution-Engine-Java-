package com.flowforge.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.flowforge.core.dag.DagValidator;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.retry.FailureInfo;
import com.flowforge.core.retry.RetryDecision;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.state.TaskStateMachine;
import com.flowforge.core.state.TaskStatus;
import com.flowforge.core.state.WorkflowStateMachine;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.core.workflow.InvalidWorkflowDefinitionException;
import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.core.workflow.WorkflowDefinitionValidator;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.random.RandomGenerator;

/**
 * Runs workflows in one JVM, concurrently, with the same semantics the distributed engine has later: dependencies,
 * priority, bounded parallelism, timeouts, retries with backoff, pause, resume and cancel.
 *
 * <p><strong>Concurrency model: a single writer.</strong> One coordinator thread owns all execution state and
 * handles one {@link EngineEvent} at a time from a queue. Worker threads only run executors and post what happened;
 * timers and callers only post too. Because nothing else touches the state, there are no locks around it, and
 * "who changed this first" questions can't arise: the order of the queue is the order of history. The database
 * version gets the same effect with a row lock per execution.
 *
 * <p>Stale results are fenced by attempt number: a result only counts if the task is still RUNNING that same
 * attempt, so a timed-out attempt that finishes late is recorded and ignored.
 *
 * <p>Pause stops new work: nothing is dispatched, and a queued task that reaches a worker while the execution isn't
 * RUNNING is turned away ("claim rejected") and handed out again on resume. As with the database claim, only an attempt that
 * checked the flag just before the pause landed can still start.
 */
public final class LocalWorkflowEngine implements AutoCloseable {

    /** Prefix of every platform thread the engine creates, so tests can prove none leak. */
    public static final String THREAD_PREFIX = "ff-local-";

    private final TaskExecutorRegistry registry;
    private final LocalEngineOptions options;
    private final Clock clock;
    private final RandomGenerator random;
    private final DagValidator validator;

    private final BlockingQueue<EngineEvent> events = new LinkedBlockingQueue<>();
    /** Coordinator-only: executions changed since their snapshot was last published. */
    private final Set<Execution> dirty = new HashSet<>();
    private final Map<UUID, Execution> executions = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final ExecutionStrategy strategy;
    private final ScheduledExecutorService timer;
    private final Thread coordinator;
    private volatile boolean closed;
    /** Set if an Error killed the coordinator: nothing would process new executions, so start() refuses them. */
    private volatile boolean coordinatorDead;

    /**
     * @param random used for retry jitter; only the coordinator thread uses it, so it needn't be thread-safe
     */
    public LocalWorkflowEngine(TaskExecutorRegistry registry, LocalEngineOptions options, Clock clock, RandomGenerator random) {
        this.registry = registry;
        this.options = options;
        this.clock = clock;
        this.random = random;
        // The 200-task limit is an API rule; the in-process engine also runs larger benchmark graphs.
        this.validator = new DagValidator(new WorkflowDefinitionValidator(registry, Integer.MAX_VALUE));
        this.strategy = ExecutionStrategy.create(options.mode(), options.parallelism());
        ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, THREAD_PREFIX + "timer");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.setRemoveOnCancelPolicy(true); // finished attempts cancel their timeout; don't keep them queued
        this.timer = scheduler;
        this.coordinator = new Thread(this::coordinate, THREAD_PREFIX + "coordinator");
        this.coordinator.setDaemon(true);
        this.coordinator.start();
    }

    /**
     * Validates and starts a workflow. Throws {@link InvalidWorkflowDefinitionException} for a bad definition.
     * Synchronized with {@link #close()} so an execution can't slip in after shutdown and never complete.
     */
    public synchronized ExecutionHandle start(WorkflowDefinition definition, JsonNode input) {
        if (closed || coordinatorDead) {
            throw new IllegalStateException(closed ? "engine is closed" : "engine coordinator died; close and recreate it");
        }
        List<ValidationError> errors = validator.validate(definition);
        if (!errors.isEmpty()) {
            throw new InvalidWorkflowDefinitionException(errors);
        }
        Execution execution = new Execution(definition, input, random);
        executions.put(execution.id, execution);
        post(new EngineEvent.Start(execution));
        return new ExecutionHandle(execution, this);
    }

    void post(EngineEvent event) {
        events.add(event);
    }

    /** Attempts running right now; tests use it to prove nothing leaks after {@link #close()}. */
    int activeWorkers() {
        return strategy.activeWorkers();
    }

    /**
     * Stops accepting work, cancels every attempt's token, gives running tasks {@code shutdownGrace} to stop on
     * their own, then interrupts them. Unfinished executions complete exceptionally. Idempotent.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        post(new EngineEvent.Shutdown());
        try {
            coordinator.join(TimeUnit.SECONDS.toMillis(5));
            strategy.shutdown(options.shutdownGrace());
            timer.shutdownNow();
            timer.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---- coordinator thread ----------------------------------------------------------------------------------

    private void coordinate() {
        while (true) {
            EngineEvent event;
            try {
                event = events.take();
            } catch (InterruptedException e) {
                return;
            }
            if (event instanceof EngineEvent.Shutdown) {
                abortAll();
                return;
            }
            Execution execution = executionOf(event);
            if (execution == null) {
                continue; // a late event for an execution that is finished and forgotten
            }
            try {
                apply(execution, event);
                completeIfDone(execution);
            } catch (RuntimeException bug) {
                // A bug must not kill the coordinator for every other execution; fail just this one, loudly, and stop
                // its attempts so their late events let it be forgotten.
                execution.completion.completeExceptionally(bug);
                execution.active.values().forEach(AttemptControl::cancelAndInterrupt);
            } catch (Error fatal) {
                // An Error (e.g. out of memory) leaves no safe state to continue from: fail everything and stop, and
                // refuse new executions, which nobody would process. close() still shuts the threads down.
                coordinatorDead = true;
                executions.values().forEach(e -> e.completion.completeExceptionally(fatal));
                throw fatal;
            }
            // Copying the state costs O(size), so changed executions are published together whenever the queue drains
            // (and at once on completion), not after every single event.
            dirty.add(execution);
            if (execution.completion.isDone()) {
                execution.publish();
                dirty.remove(execution);
            }
            if (events.isEmpty()) {
                dirty.forEach(Execution::publish);
                dirty.clear();
            }
            forgetIfSettled(execution);
        }
    }

    /** Drops a finished execution once no thread can report on it any more, so a long-lived engine doesn't grow. */
    private void forgetIfSettled(Execution e) {
        if (e.completion.isDone() && e.zombies.isEmpty() && !e.any(TaskStatus.RUNNING)) {
            e.publish();
            executions.remove(e.id);
        }
    }

    private Execution executionOf(EngineEvent event) {
        UUID id = switch (event) {
            case EngineEvent.Start start -> start.execution().id;
            case EngineEvent.AttemptStarted started -> started.executionId();
            case EngineEvent.AttemptFinished finished -> finished.executionId();
            case EngineEvent.ClaimRejected rejected -> rejected.executionId();
            case EngineEvent.DeadlineReached deadline -> deadline.executionId();
            case EngineEvent.RetryDue due -> due.executionId();
            case EngineEvent.Command command -> command.executionId();
            case EngineEvent.Shutdown shutdown -> null;
        };
        return id == null ? null : executions.get(id);
    }

    private void apply(Execution e, EngineEvent event) {
        switch (event) {
            case EngineEvent.Start start -> begin(e);
            case EngineEvent.AttemptStarted started -> onStarted(e, started);
            case EngineEvent.AttemptFinished finished -> onFinished(e, finished);
            case EngineEvent.ClaimRejected rejected -> onClaimRejected(e, rejected);
            case EngineEvent.DeadlineReached deadline -> onDeadline(e, deadline);
            case EngineEvent.RetryDue due -> onRetryDue(e, due);
            case EngineEvent.Command command -> onCommand(e, command.kind());
            case EngineEvent.Shutdown shutdown -> throw new IllegalStateException("handled by the loop");
        }
    }

    private void begin(Execution e) {
        setWorkflow(e, WorkflowStatus.RUNNING);
        for (String root : e.dag.roots()) {
            setTask(e, root, TaskStatus.READY);
        }
        dispatch(e);
    }

    /** READY → QUEUED for every ready task (in key order), each as a new attempt; only while RUNNING. */
    private void dispatch(Execution e) {
        if (e.status != WorkflowStatus.RUNNING) {
            return;
        }
        List<String> ready = e.statuses.entrySet().stream()
            .filter(entry -> entry.getValue() == TaskStatus.READY).map(Map.Entry::getKey).sorted().toList();
        for (String key : ready) {
            setTask(e, key, TaskStatus.QUEUED);
            int attempt = e.attempts.merge(key, 1, Integer::sum);
            record(e, key, attempt, TimelineEvent.Type.QUEUED, null);

            AttemptControl control = new AttemptControl();
            e.active.put(key, control);
            TaskDefinition task = e.definitions.get(key);
            Map<String, JsonNode> upstream = new HashMap<>();
            for (String dependency : e.dag.dependencies(key)) {
                upstream.put(dependency, e.outputs.get(dependency));
            }
            if (e.input != null) {
                upstream.put(TaskContext.INPUT_KEY, e.input);
            }
            UUID executionId = e.id;
            UUID taskId = e.taskIds.get(key);
            BooleanSupplier claimsOpen = () -> e.acceptingClaims; // the only execution field a worker reads
            PrioritizedTask runnable = new PrioritizedTask(task.priority().weight(), sequence.incrementAndGet(),
                () -> runAttempt(executionId, taskId, task, attempt, upstream, control, claimsOpen));
            e.queued.put(key, runnable);
            strategy.submit(runnable);
        }
    }

    private void onClaimRejected(Execution e, EngineEvent.ClaimRejected rejected) {
        if (!isCurrent(e, rejected.taskKey(), rejected.attempt(), TaskStatus.QUEUED)) {
            return;
        }
        if (e.status == WorkflowStatus.RUNNING) {
            // The worker saw the flag while paused, but a resume was processed before this event: hand it out now,
            // or nothing would ever re-submit it.
            strategy.submit(e.queued.get(rejected.taskKey()));
        } else {
            e.parked.add(rejected.taskKey());
        }
    }

    private void onStarted(Execution e, EngineEvent.AttemptStarted started) {
        if (isCurrent(e, started.taskKey(), started.attempt(), TaskStatus.QUEUED)) {
            e.queued.remove(started.taskKey());
            setTask(e, started.taskKey(), TaskStatus.RUNNING);
            record(e, started.taskKey(), started.attempt(), TimelineEvent.Type.STARTED, null);
        }
    }

    private void onFinished(Execution e, EngineEvent.AttemptFinished finished) {
        String key = finished.taskKey();
        int attempt = finished.attempt();
        if (!isCurrent(e, key, attempt, TaskStatus.RUNNING)) {
            e.zombies.remove(key + "#" + attempt);
            record(e, key, attempt, TimelineEvent.Type.STALE_RESULT_IGNORED, null);
            return;
        }
        AttemptControl control = e.active.remove(key);
        TaskResult result = finished.result();
        if (finished.error() != null) {
            // Interrupted because we cancelled it is a cancellation; any other exception is an executor bug. Only the
            // exception class is kept: its message could contain task data.
            boolean cancelled = finished.error() instanceof InterruptedException && control.token.isCancelled();
            result = cancelled ? TaskResult.cancelled()
                : TaskResult.failure("EXECUTOR_ERROR", finished.error().getClass().getSimpleName(), false);
        } else if (result == null) {
            result = TaskResult.failure("EXECUTOR_ERROR", "executor returned no result", false);
        }
        switch (result) {
            case TaskResult.Success success -> succeed(e, key, attempt, success);
            case TaskResult.Failure failure -> fail(e, key, attempt,
                FailureInfo.failed(failure.errorCode(), failure.retryable()), TimelineEvent.Type.FAILED);
            case TaskResult.Cancelled cancelled -> {
                if (e.status == WorkflowStatus.CANCELLING) {
                    cancelTask(e, key, attempt);
                } else {
                    // The executor gave up on its own; there's no user cancel to honour, so it's a failure.
                    fail(e, key, attempt, FailureInfo.of(AttemptOutcome.CANCELLED), TimelineEvent.Type.FAILED);
                }
            }
        }
    }

    private void succeed(Execution e, String key, int attempt, TaskResult.Success success) {
        setTask(e, key, TaskStatus.SUCCEEDED);
        e.outputs.put(key, success.output() == null ? NullNode.getInstance() : success.output());
        record(e, key, attempt, TimelineEvent.Type.SUCCEEDED, null);
        for (String dependent : e.readiness.newlyReady(key, e.statuses)) {
            setTask(e, dependent, TaskStatus.READY);
        }
        // A late success during CANCELLING is recorded, but the execution still ends CANCELLED.
        boolean active = e.status == WorkflowStatus.RUNNING || e.status == WorkflowStatus.PAUSED;
        if (active && e.statuses.values().stream().allMatch(s -> s == TaskStatus.SUCCEEDED)) {
            setWorkflow(e, WorkflowStatus.COMPLETED);
        }
        dispatch(e);
    }

    private void fail(Execution e, String key, int attempt, FailureInfo failure, TimelineEvent.Type type) {
        record(e, key, attempt, type, failure.errorCode());
        if (e.status == WorkflowStatus.CANCELLING) {
            cancelTask(e, key, attempt);
            return;
        }
        RetryDecision decision = e.retryPolicies.get(key).decide(e.retryCounts.get(key), failure);
        switch (decision) {
            case RetryDecision.Retry retry -> {
                setTask(e, key, TaskStatus.RETRYING);
                e.retryCounts.merge(key, 1, Integer::sum);
                record(e, key, attempt, TimelineEvent.Type.RETRY_SCHEDULED, retry.delay().toString());
                schedule(retry.delay(), new EngineEvent.RetryDue(e.id, key, attempt));
            }
            case RetryDecision.GiveUp giveUp -> {
                setTask(e, key, TaskStatus.FAILED_PERMANENTLY);
                record(e, key, attempt, TimelineEvent.Type.FAILED_PERMANENTLY, giveUp.reason());
                if (e.status == WorkflowStatus.RUNNING || e.status == WorkflowStatus.PAUSED) {
                    setWorkflow(e, WorkflowStatus.FAILED);
                }
            }
        }
    }

    private void onDeadline(Execution e, EngineEvent.DeadlineReached deadline) {
        String key = deadline.taskKey();
        if (!isCurrent(e, key, deadline.attempt(), TaskStatus.RUNNING)) {
            return; // finished in time; the timer fired anyway
        }
        e.active.remove(key).cancelAndInterrupt();
        e.zombies.add(key + "#" + deadline.attempt());
        fail(e, key, deadline.attempt(), FailureInfo.of(AttemptOutcome.TIMED_OUT), TimelineEvent.Type.TIMED_OUT);
    }

    private void onRetryDue(Execution e, EngineEvent.RetryDue due) {
        if (isCurrent(e, due.taskKey(), due.attempt(), TaskStatus.RETRYING)) {
            setTask(e, due.taskKey(), TaskStatus.READY);
            dispatch(e);
        }
    }

    private void onCommand(Execution e, EngineEvent.Command.Kind kind) {
        switch (kind) {
            case PAUSE -> {
                if (e.status == WorkflowStatus.RUNNING) {
                    setWorkflow(e, WorkflowStatus.PAUSED);
                }
            }
            case RESUME -> {
                if (e.status == WorkflowStatus.PAUSED) {
                    setWorkflow(e, WorkflowStatus.RUNNING);
                    for (String key : e.parked.stream().sorted().toList()) {
                        strategy.submit(e.queued.get(key)); // same attempt, handed out again
                    }
                    e.parked.clear();
                    dispatch(e);
                }
            }
            case CANCEL -> {
                if (e.status == WorkflowStatus.RUNNING || e.status == WorkflowStatus.PAUSED) {
                    setWorkflow(e, WorkflowStatus.CANCELLING);
                    cancelWaitingAndSignalRunning(e);
                }
            }
        }
    }

    /** Waiting tasks are cancelled outright; running ones get their token cancelled and report back. */
    private void cancelWaitingAndSignalRunning(Execution e) {
        for (Map.Entry<String, TaskStatus> task : List.copyOf(e.statuses.entrySet())) {
            String key = task.getKey();
            switch (task.getValue()) {
                case PENDING, READY, RETRYING -> cancelTask(e, key, e.attempts.get(key));
                case QUEUED -> {
                    e.active.remove(key).token.cancel(); // the queued runnable will see it and not start
                    e.queued.remove(key);
                    e.parked.remove(key);
                    cancelTask(e, key, e.attempts.get(key));
                }
                case RUNNING -> e.active.get(key).cancelAndInterrupt(); // also unblocks tasks stuck in I/O or waits
                default -> {
                }
            }
        }
    }

    private void cancelTask(Execution e, String key, int attempt) {
        setTask(e, key, TaskStatus.CANCELLED);
        record(e, key, attempt, TimelineEvent.Type.CANCELLED, null);
    }

    /** Settles CANCELLING, and completes the caller's future once nothing can change any more. */
    private void completeIfDone(Execution e) {
        if (e.status == WorkflowStatus.CANCELLING && !e.any(TaskStatus.RUNNING)) {
            setWorkflow(e, WorkflowStatus.CANCELLED);
        }
        boolean done = switch (e.status) {
            case COMPLETED, CANCELLED -> true;
            // QUEUED tasks of a FAILED execution are turned away at claim time and parked; once all are, it's settled.
            // (A key leaves `queued` only when its attempt starts or is cancelled, so this covers every QUEUED task.)
            case FAILED -> !e.any(TaskStatus.RUNNING, TaskStatus.RETRYING) && e.parked.containsAll(e.queued.keySet());
            default -> false;
        };
        if (done && !e.completion.isDone()) {
            e.publish();
            e.completion.complete(e.snapshot);
        }
    }

    private void abortAll() {
        for (Execution e : executions.values()) {
            e.active.values().forEach(control -> control.token.cancel());
            e.publish();
            e.completion.completeExceptionally(new CancellationException("engine closed"));
        }
    }

    private static boolean isCurrent(Execution e, String key, int attempt, TaskStatus expected) {
        return e.statuses.get(key) == expected && e.attempts.get(key) == attempt;
    }

    /** Every status change goes through the core state machines. */
    private void setTask(Execution e, String key, TaskStatus to) {
        e.statuses.put(key, TaskStateMachine.INSTANCE.transition(e.taskIds.get(key), e.statuses.get(key), to));
    }

    private void setWorkflow(Execution e, WorkflowStatus to) {
        e.status = WorkflowStateMachine.INSTANCE.transition(e.id, e.status, to);
        e.acceptingClaims = e.status == WorkflowStatus.RUNNING;
        record(e, null, 0, TimelineEvent.Type.WORKFLOW_STATUS, to.name());
    }

    private void record(Execution e, String key, int attempt, TimelineEvent.Type type, String detail) {
        e.record(clock.instant(), key, attempt, type, detail);
    }

    private void schedule(Duration delay, EngineEvent event) {
        try {
            timer.schedule(() -> post(event), delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException closing) {
            // The engine is shutting down; the event no longer matters.
        }
    }

    // ---- worker threads ----------------------------------------------------------------------------------------

    /** Runs one attempt on a worker thread and reports back; never touches execution state. */
    private void runAttempt(UUID executionId, UUID taskId, TaskDefinition task, int attempt, Map<String, JsonNode> upstream,
                            AttemptControl control, BooleanSupplier claimsOpen) {
        if (control.token.isCancelled()) {
            return; // cancelled while waiting in the queue: nothing new starts
        }
        if (!claimsOpen.getAsBoolean()) {
            post(new EngineEvent.ClaimRejected(executionId, task.key(), attempt)); // paused or failed: don't start
            return;
        }
        control.bind(Thread.currentThread());
        post(new EngineEvent.AttemptStarted(executionId, task.key(), attempt));
        ScheduledFuture<?> deadlineTimer;
        try {
            deadlineTimer = timer.schedule(() -> post(new EngineEvent.DeadlineReached(executionId, task.key(), attempt)),
                task.timeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException closing) {
            control.finish();
            return;
        }
        TaskResult result = null;
        Throwable error = null;
        try {
            TaskContext context = new TaskContext(executionId, taskId, task.key(), attempt, task.config(), upstream,
                clock.instant().plus(task.timeout()), control.token, "local-" + executionId);
            result = registry.get(task.type()).execute(context);
        } catch (Throwable t) {
            // Throwable, not Exception: an Error that escaped would end the attempt silently and hang the workflow.
            error = t;
        } finally {
            deadlineTimer.cancel(false);
            control.finish();
        }
        post(new EngineEvent.AttemptFinished(executionId, task.key(), attempt, result, error));
    }
}

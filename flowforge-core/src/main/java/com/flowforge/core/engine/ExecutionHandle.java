package com.flowforge.core.engine;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The caller's view of one running execution. Commands are posted to the engine's coordinator and applied in
 * order with everything else, so they never race with task completions.
 */
public final class ExecutionHandle {

    private final UUID id;
    private final Execution execution;
    private final LocalWorkflowEngine engine;

    ExecutionHandle(Execution execution, LocalWorkflowEngine engine) {
        this.id = execution.id;
        this.execution = execution;
        this.engine = engine;
    }

    public UUID id() {
        return id;
    }

    /**
     * Completes when the execution is COMPLETED or CANCELLED, or FAILED with nothing left running; completes
     * exceptionally if the engine is closed first.
     */
    public CompletableFuture<ExecutionReport> completion() {
        return execution.completion;
    }

    /**
     * Stops new work: nothing is dispatched and queued tasks don't start (only an attempt already starting at that
     * instant may slip through); running tasks finish. Ignored unless RUNNING.
     */
    public void pause() {
        engine.post(new EngineEvent.Command(id, EngineEvent.Command.Kind.PAUSE));
    }

    /** Resumes dispatching. Ignored unless PAUSED. */
    public void resume() {
        engine.post(new EngineEvent.Command(id, EngineEvent.Command.Kind.RESUME));
    }

    /**
     * Cancels waiting tasks and asks running ones to stop (token + interrupt); the execution ends CANCELLED once
     * nothing runs. Ignored unless RUNNING or PAUSED.
     */
    public void cancel() {
        engine.post(new EngineEvent.Command(id, EngineEvent.Command.Kind.CANCEL));
    }

    /**
     * A consistent picture of the execution. Safe to call from any thread. It can lag behind while the coordinator
     * still has events queued (it publishes every changed execution whenever its queue drains, and at once on
     * completion), so poll it rather than assume it's current.
     */
    public ExecutionReport snapshot() {
        return execution.snapshot;
    }
}

package com.flowforge.core.engine;

import com.flowforge.core.executor.TaskResult;
import java.util.UUID;

/**
 * Everything that can change engine state, as messages to the coordinator thread. Worker threads, timers and
 * callers only ever post these; they never touch execution state directly (single-writer principle).
 */
sealed interface EngineEvent {

    record Start(Execution execution) implements EngineEvent {
    }

    record AttemptStarted(UUID executionId, String taskKey, int attempt) implements EngineEvent {
    }

    /** {@code result} is null if the executor threw {@code error}. */
    record AttemptFinished(UUID executionId, String taskKey, int attempt, TaskResult result, Throwable error)
        implements EngineEvent {
    }

    /** A queued task reached a worker while the execution wasn't RUNNING; it stays QUEUED until resume. */
    record ClaimRejected(UUID executionId, String taskKey, int attempt) implements EngineEvent {
    }

    record DeadlineReached(UUID executionId, String taskKey, int attempt) implements EngineEvent {
    }

    record RetryDue(UUID executionId, String taskKey, int attempt) implements EngineEvent {
    }

    record Command(UUID executionId, Kind kind) implements EngineEvent {
        enum Kind { PAUSE, RESUME, CANCEL }
    }

    record Shutdown() implements EngineEvent {
    }
}

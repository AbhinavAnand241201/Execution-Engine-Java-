package com.flowforge.core.engine;

import java.time.Instant;

/**
 * One thing that happened in a local execution, in the order the coordinator processed it. Tests and the demo read
 * the timeline instead of poking at engine internals.
 *
 * @param taskKey {@code null} for workflow-level events
 * @param detail  free text: error code, retry delay (ISO-8601 duration), or new workflow status
 */
public record TimelineEvent(Instant at, String taskKey, int attempt, Type type, String detail) {

    public enum Type {
        WORKFLOW_STATUS,
        QUEUED,
        STARTED,
        SUCCEEDED,
        FAILED,
        TIMED_OUT,
        RETRY_SCHEDULED,
        FAILED_PERMANENTLY,
        CANCELLED,
        /** A result arrived for an attempt that is no longer current (e.g. after its timeout) and was dropped. */
        STALE_RESULT_IGNORED
    }
}

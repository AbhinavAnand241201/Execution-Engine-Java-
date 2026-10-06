package com.flowforge.orchestrator.api;

import java.time.Instant;
import java.util.List;

/**
 * The one error body every endpoint returns. Clients branch on {@code error} (a stable code), show
 * {@code message} to people, and quote {@code correlationId} when reporting a problem. Internals never appear here.
 *
 * @param details per-field problems; omitted from the JSON when there are none
 */
public record ApiError(Instant timestamp, int status, String error, String message, String path, String correlationId,
                            List<ErrorDetail> details) {

    /** One field-level problem, e.g. {@code tasks[2].dependsOn[0]} / {@code UNKNOWN_DEPENDENCY}. */
    public record ErrorDetail(String field, String code, String message) {
    }
}

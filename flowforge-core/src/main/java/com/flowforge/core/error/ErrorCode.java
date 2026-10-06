package com.flowforge.core.error;

/**
 * Every error FlowForge can report to a client. Lives in core so domain code can say <em>what</em> went
 * wrong without depending on a web framework. The HTTP status is kept next to each code on purpose: this enum is the
 * single code-to-status table, so the API layer never needs its own mapping.
 */
public enum ErrorCode {
    VALIDATION_FAILED(400),
    MALFORMED_REQUEST(400),
    INVALID_SORT(400),
    WEAK_PASSWORD(400),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    NOT_FOUND(404),
    // The next three are not business errors; they let protocol errors keep their real status in the standard body.
    METHOD_NOT_ALLOWED(405),
    NOT_ACCEPTABLE(406),
    UNSUPPORTED_MEDIA_TYPE(415),
    EMAIL_TAKEN(409),
    WORKFLOW_NAME_TAKEN(409),
    VERSION_CONFLICT(409),
    ILLEGAL_STATE_TRANSITION(409),
    WORKFLOW_NOT_PUBLISHED(409),
    NO_DRAFT_TO_PUBLISH(409),
    PUBLISHED_VERSION_IMMUTABLE(409),
    ALREADY_REPLAYED(409),
    OTHER_TASKS_FAILED(409),
    PAYLOAD_TOO_LARGE(413),
    INVALID_WORKFLOW_DEFINITION(422),
    IDEMPOTENCY_KEY_REUSED(422),
    RATE_LIMITED(429),
    INTERNAL_ERROR(500),
    SERVICE_UNAVAILABLE(503);

    private final int httpStatus;

    ErrorCode(int httpStatus) {
        this.httpStatus = httpStatus;
    }

    public int httpStatus() {
        return httpStatus;
    }
}

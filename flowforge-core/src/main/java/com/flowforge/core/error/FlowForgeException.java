package com.flowforge.core.error;

/**
 * Base of every expected, client-reportable failure. Carrying an {@link ErrorCode} lets one exception handler turn
 * any domain failure into the standard error body without a chain of {@code instanceof} checks. The message must be
 * safe to show to the client; internal details belong in logs, not here.
 */
public abstract class FlowForgeException extends RuntimeException {

    private final ErrorCode errorCode;

    protected FlowForgeException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected FlowForgeException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}

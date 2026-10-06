package com.flowforge.orchestrator.api;

import com.flowforge.core.error.ErrorCode;

/**
 * Maps an HTTP status chosen by the framework (Spring MVC, the servlet container) to our {@link ErrorCode}, so
 * those responses use the standard body too. Unmapped client errors become MALFORMED_REQUEST and unmapped server
 * errors INTERNAL_ERROR, and the response status is rewritten to match (e.g. a framework 409 or 410 reaches the
 * client as 400). Our own code never relies on this path: it throws {@code FlowForgeException} with a precise code.
 */
final class HttpErrorCodes {

    private HttpErrorCodes() {
    }

    static ErrorCode forStatus(int status) {
        return switch (status) {
            case 401 -> ErrorCode.UNAUTHORIZED;
            case 403 -> ErrorCode.FORBIDDEN;
            case 404 -> ErrorCode.NOT_FOUND;
            case 405 -> ErrorCode.METHOD_NOT_ALLOWED;
            case 406 -> ErrorCode.NOT_ACCEPTABLE;
            case 413 -> ErrorCode.PAYLOAD_TOO_LARGE;
            case 415 -> ErrorCode.UNSUPPORTED_MEDIA_TYPE;
            case 429 -> ErrorCode.RATE_LIMITED;
            case 503 -> ErrorCode.SERVICE_UNAVAILABLE;
            default -> status >= 500 ? ErrorCode.INTERNAL_ERROR : ErrorCode.MALFORMED_REQUEST;
        };
    }

    /** Client-safe default messages; exception messages never reach the client. */
    static String messageFor(ErrorCode code) {
        return switch (code) {
            case UNAUTHORIZED -> "Authentication required";
            case FORBIDDEN -> "Access denied";
            case NOT_FOUND -> "No such resource";
            case METHOD_NOT_ALLOWED -> "Method not supported here";
            case NOT_ACCEPTABLE -> "Cannot produce a response in the requested format";
            case PAYLOAD_TOO_LARGE -> "Request is too large";
            case UNSUPPORTED_MEDIA_TYPE -> "Content type not supported; use application/json";
            case RATE_LIMITED -> "Too many requests";
            case SERVICE_UNAVAILABLE -> "Service temporarily unavailable";
            case INTERNAL_ERROR -> "Internal error";
            default -> "Bad request";
        };
    }
}

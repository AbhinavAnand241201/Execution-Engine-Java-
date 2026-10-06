package com.flowforge.orchestrator.api;

import com.flowforge.core.error.ErrorCode;
import jakarta.servlet.RequestDispatcher;
import jakarta.servlet.http.HttpServletRequest;
import java.time.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Replaces Spring Boot's {@code /error} page. Errors that never reach {@link GlobalExceptionHandler} end up here:
 * exceptions thrown by servlet filters and {@code response.sendError(...)} calls (e.g. from security filters). Without
 * this, those responses would have a different shape and no correlation id.
 */
@RestController
public class ApiErrorController implements ErrorController {

    private static final Logger log = LoggerFactory.getLogger(ApiErrorController.class);

    private final Clock clock;

    public ApiErrorController(Clock clock) {
        this.clock = clock;
    }

    @RequestMapping("/error")
    public ResponseEntity<ApiError> error(HttpServletRequest request) {
        Object statusAttribute = request.getAttribute(RequestDispatcher.ERROR_STATUS_CODE);
        int status = statusAttribute instanceof Integer code ? code : 500;
        ErrorCode errorCode = HttpErrorCodes.forStatus(status);
        Object pathAttribute = request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI);
        String path = pathAttribute == null ? request.getRequestURI() : pathAttribute.toString();
        String correlationId = CorrelationIdFilter.currentId(request);
        if (errorCode.httpStatus() >= 500) {
            logWithCorrelationId(correlationId, path, request.getAttribute(RequestDispatcher.ERROR_EXCEPTION));
        }
        ApiError body = new ApiError(clock.instant(), errorCode.httpStatus(), errorCode.name(),
            HttpErrorCodes.messageFor(errorCode), path, correlationId, null);
        return ResponseEntity.status(errorCode.httpStatus()).body(body);
    }

    /**
     * The container logs a filter's exception only after the correlation filter has cleared the MDC; log it here
     * with the id, so the id a client quotes from the 500 body can be found in the logs.
     */
    private static void logWithCorrelationId(String correlationId, String path, Object exception) {
        if (correlationId != null) {
            MDC.put(CorrelationIdFilter.MDC_KEY, correlationId);
        }
        try {
            log.error("unhandled error path={}", path, exception instanceof Throwable t ? t : null);
        } finally {
            MDC.remove(CorrelationIdFilter.MDC_KEY);
        }
    }
}

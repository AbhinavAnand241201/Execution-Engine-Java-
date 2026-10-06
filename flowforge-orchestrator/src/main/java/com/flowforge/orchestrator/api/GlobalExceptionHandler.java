package com.flowforge.orchestrator.api;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;
import com.flowforge.core.workflow.InvalidWorkflowDefinitionException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.resource.NoResourceFoundException;

/**
 * Turns every exception raised inside Spring MVC into the standard error body; {@link ApiErrorController}
 * does the same for errors outside it. Expected failures carry an {@link ErrorCode} and a client-safe message.
 * Spring's own status exceptions keep their status. Anything else becomes a bare 500, so stack traces and exception
 * messages (which can contain SQL, hosts or data) never reach the client; they are logged with the correlation id.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    private final Clock clock;

    public GlobalExceptionHandler(Clock clock) {
        this.clock = clock;
    }

    @ExceptionHandler(InvalidWorkflowDefinitionException.class)
    ResponseEntity<ApiError> invalidDefinition(InvalidWorkflowDefinitionException e, HttpServletRequest request) {
        List<ApiError.ErrorDetail> details = e.errors().stream()
            .map(error -> new ApiError.ErrorDetail(error.path(), error.code().name(), error.message()))
            .toList();
        return respond(e.errorCode(), e.getMessage(), details, request);
    }

    @ExceptionHandler(FlowForgeException.class)
    ResponseEntity<ApiError> domain(FlowForgeException e, HttpServletRequest request) {
        return respond(e.errorCode(), e.getMessage(), null, request);
    }

    /** From {@code @PreAuthorize}: authenticated but not allowed. */
    @ExceptionHandler(AccessDeniedException.class)
    ResponseEntity<ApiError> accessDenied(AccessDeniedException e, HttpServletRequest request) {
        return respond(ErrorCode.FORBIDDEN, HttpErrorCodes.messageFor(ErrorCode.FORBIDDEN), null, request);
    }

    @ExceptionHandler(AuthenticationException.class)
    ResponseEntity<ApiError> unauthenticated(AuthenticationException e, HttpServletRequest request) {
        return respond(ErrorCode.UNAUTHORIZED, HttpErrorCodes.messageFor(ErrorCode.UNAUTHORIZED), null, request);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ApiError> invalidBody(MethodArgumentNotValidException e, HttpServletRequest request) {
        List<ApiError.ErrorDetail> details = e.getBindingResult().getFieldErrors().stream()
            .map(error -> new ApiError.ErrorDetail(error.getField(), constraintCode(error), error.getDefaultMessage()))
            .toList();
        return respond(ErrorCode.VALIDATION_FAILED, "Request validation failed", details, request);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    ResponseEntity<ApiError> invalidParameters(ConstraintViolationException e, HttpServletRequest request) {
        List<ApiError.ErrorDetail> details = e.getConstraintViolations().stream()
            .map(v -> new ApiError.ErrorDetail(parameterName(v), v.getConstraintDescriptor().getAnnotation()
                .annotationType().getSimpleName(), v.getMessage()))
            .toList();
        return respond(ErrorCode.VALIDATION_FAILED, "Request validation failed", details, request);
    }

    @ExceptionHandler(HandlerMethodValidationException.class)
    ResponseEntity<ApiError> invalidMethodParameters(HandlerMethodValidationException e, HttpServletRequest request) {
        List<ApiError.ErrorDetail> details = e.getParameterValidationResults().stream()
            .flatMap(result -> result.getResolvableErrors().stream().map(error -> new ApiError.ErrorDetail(
                result.getMethodParameter().getParameterName(), constraintCode(error), error.getDefaultMessage())))
            .toList();
        return respond(ErrorCode.VALIDATION_FAILED, "Request validation failed", details, request);
    }

    @ExceptionHandler({MissingServletRequestParameterException.class, MissingRequestHeaderException.class,
        MethodArgumentTypeMismatchException.class})
    ResponseEntity<ApiError> badParameter(Exception e, HttpServletRequest request) {
        return respond(ErrorCode.VALIDATION_FAILED, "A request parameter or header is missing or has the wrong type", null,
            request);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<ApiError> unreadable(HttpMessageNotReadableException e, HttpServletRequest request) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof RequestSizeLimitFilter.BodyTooLargeException) {
                return respond(ErrorCode.PAYLOAD_TOO_LARGE, "Request body is too large", null, request);
            }
        }
        return respond(ErrorCode.MALFORMED_REQUEST, "Request body is missing, is not valid JSON, or has unknown fields", null,
            request);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    ResponseEntity<ApiError> unsupportedMediaType(HttpMediaTypeNotSupportedException e, HttpServletRequest request) {
        return respond(ErrorCode.UNSUPPORTED_MEDIA_TYPE, HttpErrorCodes.messageFor(ErrorCode.UNSUPPORTED_MEDIA_TYPE), null,
            request);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    ResponseEntity<ApiError> noRoute(NoResourceFoundException e, HttpServletRequest request) {
        return respond(ErrorCode.NOT_FOUND, "No such resource", null, request);
    }

    /** RFC 9110 requires the {@code Allow} header on a 405. */
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    ResponseEntity<ApiError> wrongMethod(HttpRequestMethodNotSupportedException e, HttpServletRequest request) {
        HttpHeaders headers = new HttpHeaders();
        if (e.getSupportedHttpMethods() != null) {
            headers.setAllow(e.getSupportedHttpMethods());
        }
        return respond(ErrorCode.METHOD_NOT_ALLOWED, "Method " + e.getMethod() + " is not supported here", null, request,
            headers);
    }

    @ExceptionHandler({CannotGetJdbcConnectionException.class, CannotCreateTransactionException.class,
        DataAccessResourceFailureException.class})
    ResponseEntity<ApiError> databaseDown(Exception e, HttpServletRequest request) {
        log.error("database unavailable path={}", request.getRequestURI(), e);
        return respond(ErrorCode.SERVICE_UNAVAILABLE, HttpErrorCodes.messageFor(ErrorCode.SERVICE_UNAVAILABLE), null, request);
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> unexpected(Exception e, HttpServletRequest request) {
        // Spring's own exceptions (406, ResponseStatusException, upload limits...) already know their status.
        if (e instanceof org.springframework.web.ErrorResponse framework) {
            ErrorCode code = HttpErrorCodes.forStatus(framework.getStatusCode().value());
            if (code.httpStatus() >= 500) {
                log.error("framework error path={}", request.getRequestURI(), e);
            } else {
                log.debug("client error path={} status={}", request.getRequestURI(), framework.getStatusCode().value());
            }
            return respond(code, HttpErrorCodes.messageFor(code), null, request);
        }
        log.error("unhandled exception path={}", request.getRequestURI(), e);
        return respond(ErrorCode.INTERNAL_ERROR, HttpErrorCodes.messageFor(ErrorCode.INTERNAL_ERROR), null, request);
    }

    private ResponseEntity<ApiError> respond(ErrorCode code, String message, List<ApiError.ErrorDetail> details,
                                             HttpServletRequest request) {
        return respond(code, message, details, request, HttpHeaders.EMPTY);
    }

    private ResponseEntity<ApiError> respond(ErrorCode code, String message, List<ApiError.ErrorDetail> details,
                                             HttpServletRequest request, HttpHeaders headers) {
        ApiError body = new ApiError(clock.instant(), code.httpStatus(), code.name(), message, request.getRequestURI(),
            CorrelationIdFilter.currentId(request), details == null || details.isEmpty() ? null : details);
        return ResponseEntity.status(code.httpStatus()).headers(headers).body(body);
    }

    /** The constraint's name ({@code NotBlank}, {@code Size}), so clients get a code instead of a sentence. */
    private static String constraintCode(MessageSourceResolvable error) {
        String[] codes = error.getCodes();
        return codes == null || codes.length == 0 ? "INVALID" : codes[codes.length - 1];
    }

    /** {@code things.size} → {@code size}: the Java method name is an implementation detail. */
    private static String parameterName(ConstraintViolation<?> violation) {
        String path = violation.getPropertyPath().toString();
        int dot = path.indexOf('.');
        return dot < 0 ? path : path.substring(dot + 1);
    }
}

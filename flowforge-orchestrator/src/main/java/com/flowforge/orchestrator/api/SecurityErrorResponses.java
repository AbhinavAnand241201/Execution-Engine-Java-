package com.flowforge.orchestrator.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.error.ErrorCode;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Clock;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.stereotype.Component;

/**
 * Writes the standard error body for 401 and 403 raised by the security filters, which run before Spring MVC and so
 * never reach {@link GlobalExceptionHandler}. The body never says why a token was rejected (expired, bad signature,
 * wrong issuer): that would help an attacker tune forgeries.
 */
@Component
public class SecurityErrorResponses implements AuthenticationEntryPoint, AccessDeniedHandler {

    private final ObjectMapper json;
    private final Clock clock;

    public SecurityErrorResponses(ObjectMapper json, Clock clock) {
        this.json = json;
        this.clock = clock;
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response, AuthenticationException exception)
        throws IOException {
        response.setHeader(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
        write(request, response, ErrorCode.UNAUTHORIZED);
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response, AccessDeniedException exception)
        throws IOException {
        write(request, response, ErrorCode.FORBIDDEN);
    }

    private void write(HttpServletRequest request, HttpServletResponse response, ErrorCode code) throws IOException {
        ApiError body = new ApiError(clock.instant(), code.httpStatus(), code.name(), HttpErrorCodes.messageFor(code),
            request.getRequestURI(), CorrelationIdFilter.currentId(request), null);
        response.setStatus(code.httpStatus());
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        json.writeValue(response.getOutputStream(), body);
    }
}

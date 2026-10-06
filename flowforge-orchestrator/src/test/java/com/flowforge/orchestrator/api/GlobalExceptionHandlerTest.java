package com.flowforge.orchestrator.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Clock;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.mock.web.MockHttpServletRequest;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler(Clock.systemUTC());

    @Test
    @DisplayName("T06.extra-6 database connection failure → 503 SERVICE_UNAVAILABLE without the driver's message")
    void databaseDownIs503() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/anything");
        request.setAttribute(CorrelationIdFilter.MDC_KEY, "db-down-1");

        ResponseEntity<ApiError> response = handler.databaseDown(
            new CannotGetJdbcConnectionException("Connection to db-host:5432 refused", new SQLException("password=hunter2")),
            request);

        assertThat(response.getStatusCode().value()).isEqualTo(503);
        assertThat(response.getBody().error()).isEqualTo("SERVICE_UNAVAILABLE");
        assertThat(response.getBody().correlationId()).isEqualTo("db-down-1");
        assertThat(response.getBody().message()).doesNotContain("db-host", "hunter2");
    }
}

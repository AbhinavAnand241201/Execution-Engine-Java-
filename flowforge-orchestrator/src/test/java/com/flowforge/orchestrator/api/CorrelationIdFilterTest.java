package com.flowforge.orchestrator.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("T06.3 the id is in the MDC while the request runs, and echoed on the response")
    void idIsInMdcDuringRequest() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationIdFilter.HEADER, "abc-123");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> seen = new AtomicReference<>();

        filter.doFilter(request, response, (req, res) -> seen.set(MDC.get(CorrelationIdFilter.MDC_KEY)));

        assertThat(seen).hasValue("abc-123");
        assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isEqualTo("abc-123");
        assertThat(request.getAttribute(CorrelationIdFilter.MDC_KEY)).isEqualTo("abc-123");
    }

    @Test
    @DisplayName("T06.3 ids longer than 64 characters or with other characters are replaced")
    void invalidIdsReplaced() throws Exception {
        for (String invalid : new String[] {"a".repeat(65), "has space", "semi;colon", ""}) {
            MockHttpServletRequest request = new MockHttpServletRequest();
            request.addHeader(CorrelationIdFilter.HEADER, invalid);
            MockHttpServletResponse response = new MockHttpServletResponse();

            filter.doFilter(request, response, (req, res) -> { });

            assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isNotEqualTo(invalid).hasSize(36);
        }
    }

    @Test
    @DisplayName("T06.8 the MDC is empty after the request, even when the chain throws")
    void mdcClearedAfterRequest() {
        FilterChain ok = (req, res) -> { };
        FilterChain failing = (req, res) -> {
            throw new ServletException("boom");
        };

        assertThatCode(() -> filter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), ok))
            .doesNotThrowAnyException();
        assertThat(MDC.get(CorrelationIdFilter.MDC_KEY)).isNull();

        assertThatThrownBy(() -> filter.doFilter(new MockHttpServletRequest(), new MockHttpServletResponse(), failing))
            .isInstanceOf(ServletException.class);
        assertThat(MDC.get(CorrelationIdFilter.MDC_KEY)).isNull();
        assertThat(MDC.getCopyOfContextMap()).isNullOrEmpty();
    }
}

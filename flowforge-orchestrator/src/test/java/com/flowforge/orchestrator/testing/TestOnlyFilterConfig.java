package com.flowforge.orchestrator.testing;

import jakarta.servlet.Filter;
import jakarta.servlet.ServletException;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** A filter that fails outside Spring MVC, so tests can check the /error path keeps the standard error body. */
@Configuration
class TestOnlyFilterConfig {

    @Bean
    FilterRegistrationBean<Filter> failingFilter() {
        Filter failing = (request, response, chain) -> {
            throw new ServletException("filter secret");
        };
        FilterRegistrationBean<Filter> registration = new FilterRegistrationBean<>(failing);
        registration.addUrlPatterns("/api/v1/test/filter-boom");
        registration.setOrder(100); // after CorrelationIdFilter
        return registration;
    }
}

package com.flowforge.orchestrator.config;

import com.flowforge.orchestrator.api.CurrentUserArgumentResolver;
import com.flowforge.orchestrator.api.RequestSizeLimitFilter;
import java.util.List;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** Registers {@code @CurrentUser} so controllers receive the caller as a plain record, and the request size cap. */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addArgumentResolvers(List<HandlerMethodArgumentResolver> resolvers) {
        resolvers.add(new CurrentUserArgumentResolver());
    }

    /** Right after the correlation id filter, so even a refused body gets a correlation id. */
    @Bean
    FilterRegistrationBean<RequestSizeLimitFilter> requestSizeLimitFilter(LimitsProperties limits) {
        FilterRegistrationBean<RequestSizeLimitFilter> registration =
            new FilterRegistrationBean<>(new RequestSizeLimitFilter(limits.requestMaxBytes()));
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 1);
        return registration;
    }
}

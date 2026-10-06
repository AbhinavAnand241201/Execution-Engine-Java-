package com.flowforge.orchestrator.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.runtime.testing.IntegrationTestBase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Boots the whole orchestrator on a random port against the shared containers of {@link IntegrationTestBase}
 *. Spring caches the context across test classes with the same configuration, so most ITs share one
 * application, just as they share one set of containers.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// Boot disables metrics export in tests by default; we want /actuator/prometheus to exist as in production.
@AutoConfigureObservability(tracing = false)
@ActiveProfiles("test")
public abstract class OrchestratorIntegrationTestBase extends IntegrationTestBase {

    @Autowired
    protected TestRestTemplate rest;

    @Autowired
    protected ObjectMapper json;

    protected ApiClient api() {
        return new ApiClient(rest, json);
    }

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }
}

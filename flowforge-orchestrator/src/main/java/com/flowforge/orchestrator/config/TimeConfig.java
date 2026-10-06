package com.flowforge.orchestrator.config;

import java.time.Clock;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** One injectable clock and random source for the whole app, so tests can replace them. */
@Configuration
public class TimeConfig {

    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }

    /** Retry jitter. ThreadLocalRandom's shared instance is safe to call from any thread. */
    @Bean
    public RandomGenerator randomGenerator() {
        return ThreadLocalRandom.current();
    }
}

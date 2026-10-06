package com.flowforge.core.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * "Full jitter": the actual wait is uniformly random in {@code [0, computed]}. When many tasks fail together (a
 * downstream outage), their retries spread out instead of hitting the recovering service in synchronized waves.
 * The generator is injected so tests are deterministic.
 */
public final class FullJitter {

    private final RandomGenerator random;

    public FullJitter(RandomGenerator random) {
        this.random = Objects.requireNonNull(random, "random");
    }

    public Duration apply(Duration computed) {
        long millis = computed.toMillis();
        return millis <= 0 ? Duration.ZERO : Duration.ofMillis(random.nextLong(0, millis + 1));
    }
}

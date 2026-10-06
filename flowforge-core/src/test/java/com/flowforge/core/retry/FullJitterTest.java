package com.flowforge.core.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.SplittableRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FullJitterTest {

    @Test
    @DisplayName("T04.5 full jitter stays within [0, computed] for 10 000 seeded samples; mean ≈ computed / 2 ± 5%")
    void fullJitterIsUniform() {
        FullJitter jitter = new FullJitter(new SplittableRandom(20261001L));
        Duration computed = Duration.ofMillis(8_000);

        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (int i = 0; i < 10_000; i++) {
            long millis = jitter.apply(computed).toMillis();
            sum += millis;
            min = Math.min(min, millis);
            max = Math.max(max, millis);
        }
        double mean = sum / 10_000.0;

        assertThat(min).isGreaterThanOrEqualTo(0);
        assertThat(max).isLessThanOrEqualTo(8_000);
        assertThat(mean).isCloseTo(4_000.0, within(4_000.0 * 0.05));
    }

    @Test
    @DisplayName("T04.5 same seed, same sequence (deterministic tests); zero stays zero")
    void seededAndZero() {
        FullJitter first = new FullJitter(new SplittableRandom(7));
        FullJitter second = new FullJitter(new SplittableRandom(7));

        for (int i = 0; i < 100; i++) {
            assertThat(first.apply(Duration.ofSeconds(10))).isEqualTo(second.apply(Duration.ofSeconds(10)));
        }
        assertThat(first.apply(Duration.ZERO)).isEqualTo(Duration.ZERO);
    }

    @Test
    @DisplayName("T04.5 the upper bound is inclusive: computed = 1 ms yields both 0 and 1 ms")
    void upperBoundIsReachable() {
        FullJitter jitter = new FullJitter(new SplittableRandom(3));
        Set<Long> seen = new HashSet<>();
        for (int i = 0; i < 1_000; i++) {
            seen.add(jitter.apply(Duration.ofMillis(1)).toMillis());
        }
        assertThat(seen).containsExactlyInAnyOrder(0L, 1L);
    }
}

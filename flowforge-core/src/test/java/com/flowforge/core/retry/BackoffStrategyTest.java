package com.flowforge.core.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BackoffStrategyTest {

    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    @Test
    @DisplayName("T04.1 fixed backoff gives the same delay for every retry")
    void fixedIsConstant() {
        BackoffStrategy fixed = new FixedBackoff(Duration.ofMillis(750));

        for (int retry = 1; retry <= 20; retry++) {
            assertThat(fixed.delay(retry)).isEqualTo(Duration.ofMillis(750));
        }
    }

    @Test
    @DisplayName("T04.2 linear backoff gives n × initial, capped at max")
    void linearGrowsAndCaps() {
        BackoffStrategy linear = new LinearBackoff(ONE_SECOND, Duration.ofSeconds(5));

        assertThat(linear.delay(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(linear.delay(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(linear.delay(3)).isEqualTo(Duration.ofSeconds(3));
        assertThat(linear.delay(5)).isEqualTo(Duration.ofSeconds(5));
        assertThat(linear.delay(6)).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("T04.3 exponential backoff gives 1 s, 2 s, 4 s, 8 s, … capped at max")
    void exponentialDoublesAndCaps() {
        BackoffStrategy exponential = new ExponentialBackoff(ONE_SECOND, 2.0, Duration.ofSeconds(30));

        assertThat(exponential.delay(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(exponential.delay(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(exponential.delay(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(exponential.delay(4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(exponential.delay(5)).isEqualTo(Duration.ofSeconds(16));
        assertThat(exponential.delay(6)).isEqualTo(Duration.ofSeconds(30));
        assertThat(exponential.delay(7)).isEqualTo(Duration.ofSeconds(30));
    }

    @ParameterizedTest(name = "multiplier {0}")
    @ValueSource(doubles = {1.0, 2.0, 10.0})
    @DisplayName("T04.4 retry number 1 000 (and Integer.MAX_VALUE) gives maxDelay: no overflow, never negative")
    void hugeRetryNumberDoesNotOverflow(double multiplier) {
        Duration max = Duration.ofMillis(3_600_000);
        BackoffStrategy exponential = new ExponentialBackoff(Duration.ofMillis(600_000), multiplier, max);
        BackoffStrategy linear = new LinearBackoff(Duration.ofMillis(600_000), max);

        for (int retry : new int[] {1_000, Integer.MAX_VALUE}) {
            assertThat(exponential.delay(retry)).isEqualTo(multiplier == 1.0 ? Duration.ofMillis(600_000) : max);
            assertThat(linear.delay(retry)).isEqualTo(max);
        }
    }

    @Test
    @DisplayName("T04.extra-1 zero initial delay stays zero; retry numbers below 1 are rejected")
    void zeroDelayAndInvalidRetryNumber() {
        assertThat(new ExponentialBackoff(Duration.ZERO, 2.0, ONE_MINUTE).delay(50)).isEqualTo(Duration.ZERO);
        assertThat(new ExponentialBackoff(Duration.ZERO, 2.0, ONE_MINUTE).delay(Integer.MAX_VALUE)).isEqualTo(Duration.ZERO);
        assertThat(new LinearBackoff(Duration.ZERO, ONE_MINUTE).delay(50)).isEqualTo(Duration.ZERO);
        assertThatThrownBy(() -> new FixedBackoff(ONE_SECOND).delay(0)).isInstanceOf(IllegalArgumentException.class);
    }
}

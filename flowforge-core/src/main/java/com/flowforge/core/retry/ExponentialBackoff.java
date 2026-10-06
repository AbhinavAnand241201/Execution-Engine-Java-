package com.flowforge.core.retry;

import java.time.Duration;

/**
 * Waits {@code initial × multiplier^(n-1)} before retry {@code n}, capped at {@code max}: backs off quickly from a
 * struggling dependency. The power is computed in {@code double}, which saturates to infinity instead of wrapping
 * around like {@code long}; anything at or above {@code max} (including infinity) becomes {@code max}. A zero
 * initial delay stays zero.
 */
public record ExponentialBackoff(Duration initial, double multiplier, Duration max) implements BackoffStrategy {

    @Override
    public Duration delay(int retryNumber) {
        BackoffStrategy.requireRetryNumber(retryNumber);
        if (initial.isZero()) {
            return Duration.ZERO; // 0 × infinity would be NaN
        }
        double millis = initial.toMillis() * Math.pow(multiplier, retryNumber - 1);
        if (millis >= max.toMillis()) {
            return max;
        }
        return Duration.ofMillis(Math.round(millis)); // below max ≤ 3.6e6, far inside double's exact range
    }
}

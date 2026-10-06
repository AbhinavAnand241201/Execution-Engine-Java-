package com.flowforge.core.retry;

import java.time.Duration;

/** Waits {@code n × initial} before retry {@code n}, capped at {@code max}. */
public record LinearBackoff(Duration initial, Duration max) implements BackoffStrategy {

    @Override
    public Duration delay(int retryNumber) {
        BackoffStrategy.requireRetryNumber(retryNumber);
        long initialMs = initial.toMillis();
        long maxMs = max.toMillis();
        // Cap before multiplying: if n × initial would pass max, it's max. Within the validator's bounds the product
        // fits in a long anyway; the guard keeps the class correct for any Duration.
        if (initialMs > 0 && retryNumber > maxMs / initialMs) {
            return max;
        }
        return Duration.ofMillis(Math.min(maxMs, retryNumber * initialMs));
    }
}

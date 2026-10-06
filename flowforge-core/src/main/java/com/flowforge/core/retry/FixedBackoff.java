package com.flowforge.core.retry;

import java.time.Duration;

/** The same wait before every retry. Simple and predictable; best when failures aren't load-related. */
public record FixedBackoff(Duration delay) implements BackoffStrategy {

    @Override
    public Duration delay(int retryNumber) {
        BackoffStrategy.requireRetryNumber(retryNumber);
        return delay;
    }
}

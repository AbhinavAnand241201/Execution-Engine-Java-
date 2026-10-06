package com.flowforge.core.retry;

import java.time.Duration;

/**
 * How long to wait before a given retry, before jitter. One implementation per {@code BackoffType} (Strategy
 * pattern), so a new curve is a new class rather than another branch in the retry logic.
 */
public interface BackoffStrategy {

    /**
     * The delay before retry number {@code retryNumber} (1 = the first retry). Never negative and never above the
     * configured maximum, even for absurd retry numbers.
     */
    Duration delay(int retryNumber);

    static void requireRetryNumber(int retryNumber) {
        if (retryNumber < 1) {
            throw new IllegalArgumentException("retry numbers start at 1: " + retryNumber);
        }
    }
}

package com.flowforge.core.retry;

import java.time.Duration;

/**
 * The outcome of asking "should this failed attempt be retried?". Sealed, so a {@code switch} over it must handle
 * both cases and the compiler catches a forgotten branch.
 */
public sealed interface RetryDecision {

    /** Try again after {@code delay} (jitter already applied). */
    record Retry(Duration delay) implements RetryDecision {
    }

    /** Stop: the task becomes FAILED_PERMANENTLY and goes to the dead-letter queue. */
    record GiveUp(String reason) implements RetryDecision {
    }
}

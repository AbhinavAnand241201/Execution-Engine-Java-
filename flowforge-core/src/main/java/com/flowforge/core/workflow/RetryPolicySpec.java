package com.flowforge.core.workflow;

import java.time.Duration;

/**
 * The retry settings a user wrote for a task, as data. It is separate from the runtime {@code RetryPolicy}
 * so a definition can hold out-of-range values long enough for the validator to report them all.
 *
 * @param maxAttempts total tries including the first one
 * @param multiplier  growth factor; only meaningful for {@link BackoffType#EXPONENTIAL}
 * @param jitter      whether to apply full jitter to each computed delay
 */
public record RetryPolicySpec(int maxAttempts, BackoffType backoff, Duration initialDelay, Duration maxDelay,
                              double multiplier, boolean jitter) {

    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    public static final BackoffType DEFAULT_BACKOFF = BackoffType.EXPONENTIAL;
    public static final Duration DEFAULT_INITIAL_DELAY = Duration.ofMillis(1_000);
    public static final Duration DEFAULT_MAX_DELAY = Duration.ofMillis(60_000);
    public static final double DEFAULT_MULTIPLIER = 2.0;
    public static final boolean DEFAULT_JITTER = true;

    public static final RetryPolicySpec DEFAULT = new RetryPolicySpec(DEFAULT_MAX_ATTEMPTS, DEFAULT_BACKOFF,
        DEFAULT_INITIAL_DELAY, DEFAULT_MAX_DELAY, DEFAULT_MULTIPLIER, DEFAULT_JITTER);

    /** Missing object fields fall back to the documented defaults; primitives are always present. */
    public RetryPolicySpec {
        backoff = backoff == null ? DEFAULT_BACKOFF : backoff;
        initialDelay = initialDelay == null ? DEFAULT_INITIAL_DELAY : initialDelay;
        maxDelay = maxDelay == null ? DEFAULT_MAX_DELAY : maxDelay;
    }
}

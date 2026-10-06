package com.flowforge.core.retry;

import com.flowforge.core.workflow.RetryPolicySpec;
import java.time.Duration;
import java.util.function.UnaryOperator;
import java.util.random.RandomGenerator;

/**
 * Decides, for one task, whether a failed attempt is retried and after how long. It is a pure decision: it doesn't
 * sleep or schedule anything. The caller stores the due time ({@code next_attempt_at}) in the database and a
 * background job promotes the task when it's due, so a waiting retry holds no thread and survives restarts.
 */
public final class RetryPolicy {

    private final int maxAttempts;
    private final BackoffStrategy backoff;
    private final UnaryOperator<Duration> jitter;

    RetryPolicy(int maxAttempts, BackoffStrategy backoff, UnaryOperator<Duration> jitter) {
        this.maxAttempts = maxAttempts;
        this.backoff = backoff;
        this.jitter = jitter;
    }

    /**
     * Builds the policy a task's retry settings describe. The spec must already have passed
     * {@code WorkflowDefinitionValidator} (e.g. no negative delays). Jitter uses {@code random} when enabled.
     */
    public static RetryPolicy from(RetryPolicySpec spec, RandomGenerator random) {
        BackoffStrategy backoff = switch (spec.backoff()) {
            case FIXED -> new FixedBackoff(spec.initialDelay());
            case LINEAR -> new LinearBackoff(spec.initialDelay(), spec.maxDelay());
            case EXPONENTIAL -> new ExponentialBackoff(spec.initialDelay(), spec.multiplier(), spec.maxDelay());
        };
        UnaryOperator<Duration> jitter = spec.jitter() ? new FullJitter(random)::apply : UnaryOperator.identity();
        return new RetryPolicy(spec.maxAttempts(), backoff, jitter);
    }

    /**
     * @param retryCountSoFar failed attempts before this one in the current cycle (the {@code retry_count} column);
     *                        0 when the first attempt has just failed
     */
    public RetryDecision decide(int retryCountSoFar, FailureInfo failure) {
        if (retryCountSoFar < 0) {
            throw new IllegalArgumentException("retryCountSoFar must be >= 0: " + retryCountSoFar);
        }
        if (!failure.isRetryable()) {
            return new RetryDecision.GiveUp("not retryable: " + failure.errorCode());
        }
        // Same as "retryCountSoFar + 1 >= maxAttempts" (the attempt that just failed was the last), without the + 1.
        if (retryCountSoFar >= maxAttempts - 1) {
            return new RetryDecision.GiveUp("max attempts (" + maxAttempts + ") reached; last error: " + failure.errorCode());
        }
        return new RetryDecision.Retry(jitter.apply(backoff.delay(retryCountSoFar + 1)));
    }
}

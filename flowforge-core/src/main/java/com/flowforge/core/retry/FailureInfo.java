package com.flowforge.core.retry;

import com.flowforge.core.state.AttemptOutcome;
import java.util.Objects;

/**
 * Why an attempt didn't succeed, as far as the retry decision needs to know.
 *
 * @param executorSaysRetryable the executor's own verdict; only consulted for {@link AttemptOutcome#FAILED}. Use
 *                              {@link #isRetryable()} for the decision.
 */
public record FailureInfo(AttemptOutcome outcome, String errorCode, boolean executorSaysRetryable) {

    public FailureInfo {
        Objects.requireNonNull(outcome, "outcome");
        errorCode = errorCode == null ? outcome.name() : errorCode;
        if (outcome == AttemptOutcome.SUCCEEDED) {
            throw new IllegalArgumentException("a successful attempt is not a failure");
        }
    }

    /** A failure reported by the executor, e.g. HTTP 503 (retryable) or 400 (not). */
    public static FailureInfo failed(String errorCode, boolean executorSaysRetryable) {
        return new FailureInfo(AttemptOutcome.FAILED, errorCode, executorSaysRetryable);
    }

    /** A failure the engine observed itself (timeout, lost worker, shutdown, cancel). */
    public static FailureInfo of(AttemptOutcome outcome) {
        return new FailureInfo(outcome, outcome.name(), outcome.isAlwaysRetryable());
    }

    /** Infrastructure outcomes are always retryable; FAILED only if the executor said so; CANCELLED never. */
    public boolean isRetryable() {
        return outcome.isAlwaysRetryable() || (outcome == AttemptOutcome.FAILED && executorSaysRetryable);
    }
}

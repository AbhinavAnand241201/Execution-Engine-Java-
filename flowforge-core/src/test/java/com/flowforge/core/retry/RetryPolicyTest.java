package com.flowforge.core.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.workflow.BackoffType;
import com.flowforge.core.workflow.RetryPolicySpec;
import java.time.Duration;
import java.util.SplittableRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class RetryPolicyTest {

    private static final FailureInfo RETRYABLE = FailureInfo.failed("HTTP_503", true);
    private static final FailureInfo NON_RETRYABLE = FailureInfo.failed("HTTP_400", false);

    private static RetryPolicy policy(int maxAttempts, boolean jitter) {
        RetryPolicySpec spec = new RetryPolicySpec(maxAttempts, BackoffType.EXPONENTIAL, Duration.ofSeconds(1),
            Duration.ofSeconds(60), 2.0, jitter);
        return RetryPolicy.from(spec, new SplittableRandom(1));
    }

    @Test
    @DisplayName("T04.6 maxAttempts = 3: fail, fail → Retry, Retry; the third failure → GiveUp")
    void retriesUntilMaxAttempts() {
        RetryPolicy policy = policy(3, false);

        assertThat(policy.decide(0, RETRYABLE)).isEqualTo(new RetryDecision.Retry(Duration.ofSeconds(1)));
        assertThat(policy.decide(1, RETRYABLE)).isEqualTo(new RetryDecision.Retry(Duration.ofSeconds(2)));
        assertThat(policy.decide(2, RETRYABLE)).isInstanceOfSatisfying(RetryDecision.GiveUp.class,
            giveUp -> assertThat(giveUp.reason()).contains("3"));
    }

    @Test
    @DisplayName("T04.7 a non-retryable failure on the first attempt → GiveUp immediately")
    void nonRetryableGivesUp() {
        assertThat(policy(5, false).decide(0, NON_RETRYABLE)).isInstanceOfSatisfying(RetryDecision.GiveUp.class,
            giveUp -> assertThat(giveUp.reason()).contains("HTTP_400"));
    }

    @Test
    @DisplayName("T04.8 maxAttempts = 1 never retries, even a retryable failure")
    void singleAttemptNeverRetries() {
        assertThat(policy(1, false).decide(0, RETRYABLE)).isInstanceOf(RetryDecision.GiveUp.class);
        assertThat(policy(1, false).decide(0, FailureInfo.of(AttemptOutcome.WORKER_LOST))).isInstanceOf(RetryDecision.GiveUp.class);
    }

    @ParameterizedTest
    @EnumSource(value = AttemptOutcome.class, names = {"TIMED_OUT", "WORKER_LOST", "WORKER_SHUTDOWN"})
    @DisplayName("T04.extra-2 infrastructure outcomes are retried regardless of the executor's flag")
    void infrastructureOutcomesAlwaysRetry(AttemptOutcome outcome) {
        assertThat(policy(3, false).decide(0, FailureInfo.of(outcome))).isInstanceOf(RetryDecision.Retry.class);
    }

    @Test
    @DisplayName("T04.extra-3 a cancelled attempt is never retried; SUCCEEDED is not a failure")
    void cancelledAndSucceeded() {
        assertThat(policy(3, false).decide(0, FailureInfo.of(AttemptOutcome.CANCELLED))).isInstanceOf(RetryDecision.GiveUp.class);
        assertThatThrownBy(() -> FailureInfo.of(AttemptOutcome.SUCCEEDED)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("T04.extra-5 negative retry count is rejected; a huge one gives up without overflow")
    void inputValidation() {
        assertThatThrownBy(() -> policy(3, false).decide(-1, RETRYABLE)).isInstanceOf(IllegalArgumentException.class);
        assertThat(policy(3, false).decide(Integer.MAX_VALUE, RETRYABLE)).isInstanceOf(RetryDecision.GiveUp.class);
    }

    @Test
    @DisplayName("T04.extra-4 with jitter the delay is within [0, computed backoff]")
    void jitteredDelayWithinBounds() {
        RetryPolicy policy = policy(10, true);

        for (int retryCount = 0; retryCount < 9; retryCount++) {
            Duration computed = Duration.ofSeconds(Math.min(60, 1L << retryCount));
            assertThat(policy.decide(retryCount, RETRYABLE)).isInstanceOfSatisfying(RetryDecision.Retry.class, retry ->
                assertThat(retry.delay()).isBetween(Duration.ZERO, computed));
        }
    }
}

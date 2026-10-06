package com.flowforge.runtime.runner;

import java.time.Duration;

/**
 * @param maxConcurrency       attempts running at once (capacity permits)
 * @param acquireTimeout       how long a command waits for a permit before {@link CommandOutcome#RETRY_LATER}
 * @param publishAttempts      tries to hand a result to the sink before giving up (the reconciler/timeout sweeper
 *                             then recovers the task)
 * @param publishBackoff       first wait between tries; doubles each time
 */
public record TaskRunnerSettings(String workerId, int maxConcurrency, ThreadMode threadMode, Duration acquireTimeout,
                                 int publishAttempts, Duration publishBackoff) {

    public TaskRunnerSettings {
        if (maxConcurrency < 1 || publishAttempts < 1) {
            throw new IllegalArgumentException("maxConcurrency and publishAttempts must be >= 1");
        }
    }
}

package com.flowforge.core.engine;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The engine-level leak test can't prove this alone: {@code ThreadPoolExecutor} also clears a worker's interrupt
 * before each task. These checks exercise {@link AttemptControl} directly, on the calling thread.
 */
class AttemptControlTest {

    @Test
    @DisplayName("T05.7 an interrupt requested after the attempt finished is not delivered")
    void lateInterruptIsDropped() {
        AttemptControl control = new AttemptControl();
        control.bind(Thread.currentThread());
        control.finish();

        control.cancelAndInterrupt();

        assertThat(Thread.interrupted()).isFalse();
        assertThat(control.token.isCancelled()).isTrue();
    }

    @Test
    @DisplayName("T05.7 an interrupt delivered during the attempt is cleared when the attempt finishes")
    void interruptClearedOnFinish() {
        AttemptControl control = new AttemptControl();
        control.bind(Thread.currentThread());

        control.cancelAndInterrupt();
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        control.finish();

        assertThat(Thread.interrupted()).isFalse();
    }
}

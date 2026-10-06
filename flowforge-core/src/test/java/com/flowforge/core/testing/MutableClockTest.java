package com.flowforge.core.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MutableClockTest {

    @Test
    @DisplayName("T00.extra-1 MutableClock stays still until advanced and never moves backwards")
    void advancesOnlyWhenTold() {
        MutableClock clock = MutableClock.startingAt2026();
        Instant start = clock.instant();

        assertThat(clock.instant()).isEqualTo(start);

        clock.advance(Duration.ofSeconds(5));
        assertThat(clock.instant()).isEqualTo(start.plusSeconds(5));
        assertThat(clock.millis()).isEqualTo(start.plusSeconds(5).toEpochMilli());

        assertThatThrownBy(() -> clock.advance(Duration.ofSeconds(-1))).isInstanceOf(IllegalArgumentException.class);
    }
}

package com.flowforge.core.testing;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A UTC {@link Clock} that tests move by hand, so time-dependent code (backoff, timeouts, heartbeats) is tested
 * deterministically instead of by sleeping. Thread-safe: concurrency tests may read it from many threads.
 */
public final class MutableClock extends Clock {

    private final AtomicReference<Instant> now;

    public MutableClock(Instant start) {
        this.now = new AtomicReference<>(start);
    }

    /** A clock at 2026-01-01T00:00:00Z until moved. */
    public static MutableClock startingAt2026() {
        return new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    }

    public void advance(Duration duration) {
        if (duration.isNegative()) {
            throw new IllegalArgumentException("time only moves forward: " + duration);
        }
        now.updateAndGet(current -> current.plus(duration));
    }

    public void set(Instant instant) {
        now.set(instant);
    }

    @Override
    public Instant instant() {
        return now.get();
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    /** FlowForge works in UTC only; a clock in another zone would hide bugs, so this is refused. */
    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("MutableClock is UTC-only");
    }
}

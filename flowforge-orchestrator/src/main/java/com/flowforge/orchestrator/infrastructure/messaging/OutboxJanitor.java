package com.flowforge.orchestrator.infrastructure.messaging;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Deletes published outbox rows older than the retention (OutboxJanitor). Unpublished rows are never
 * deleted, however old: they are messages that still have to go out.
 */
public class OutboxJanitor {

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final Duration retention;

    public OutboxJanitor(JdbcTemplate jdbc, Clock clock, Duration retention) {
        this.jdbc = jdbc;
        this.clock = clock;
        this.retention = retention;
    }

    /** Returns how many rows were deleted. */
    public int cleanUp() {
        return jdbc.update("DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < ?",
            Timestamp.from(clock.instant().minus(retention)));
    }
}

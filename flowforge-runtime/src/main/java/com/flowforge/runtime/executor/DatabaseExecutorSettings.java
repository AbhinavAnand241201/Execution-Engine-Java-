package com.flowforge.runtime.executor;

import java.time.Duration;
import java.util.Map;

/**
 * {@code flowforge.executors.database.*}.
 *
 * @param queries          the only SQL a DATABASE task can run, by name; parameters are written {@code :name}
 * @param statementTimeout PostgreSQL {@code statement_timeout} for each query
 * @param maxRows          rows beyond this are not fetched
 */
public record DatabaseExecutorSettings(Map<String, String> queries, Duration statementTimeout, int maxRows) {

    public DatabaseExecutorSettings {
        if (maxRows < 1 || statementTimeout.isNegative() || statementTimeout.isZero()) {
            throw new IllegalArgumentException("max-rows and statement-timeout must be positive");
        }
        queries = Map.copyOf(queries);
    }
}

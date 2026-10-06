package com.flowforge.runtime.db;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;

/**
 * "Is the database unreachable right now?" for any exception, however it was wrapped (Spring, Kafka, our own
 * IllegalStateException). Used to tell an outage (retry until it is back) from a problem with the message itself.
 */
public final class DatabaseErrors {

    private DatabaseErrors() {
    }

    /**
     * True for connection failures (SQLState class 08), admin shutdown / crash recovery (57P01–57P03), too many
     * connections / out of resources (class 53), and the JDBC transient/recoverable exception types — anywhere in
     * the cause chain.
     */
    public static boolean isUnavailable(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLTransientException || cause instanceof SQLRecoverableException) {
                return true;
            }
            if (cause instanceof SQLException sql && sql.getSQLState() != null) {
                String state = sql.getSQLState();
                if (state.startsWith("08") || state.startsWith("53") || state.startsWith("57P0")) {
                    return true;
                }
            }
        }
        return false;
    }
}

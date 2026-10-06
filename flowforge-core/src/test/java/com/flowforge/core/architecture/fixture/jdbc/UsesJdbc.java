package com.flowforge.core.architecture.fixture.jdbc;

import java.sql.Connection;

/** Deliberate violation: a core class that depends on JDBC. Used only to prove the rule fires. */
public class UsesJdbc {

    public boolean isOpen(Connection connection) throws Exception {
        return !connection.isClosed();
    }
}

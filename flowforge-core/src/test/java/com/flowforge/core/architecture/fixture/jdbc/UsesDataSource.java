package com.flowforge.core.architecture.fixture.jdbc;

import javax.sql.DataSource;

/**
 * Deliberate violation: a core class that takes a {@code javax.sql.DataSource}, which ships with the JDK and so
 * slips past Maven's dependency graph. Used only to prove the rule fires.
 */
public class UsesDataSource {

    private final DataSource dataSource;

    public UsesDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource dataSource() {
        return dataSource;
    }
}

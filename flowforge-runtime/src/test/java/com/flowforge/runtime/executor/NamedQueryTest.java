package com.flowforge.runtime.executor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class NamedQueryTest {

    @Test
    @DisplayName("T10.extra-2 :name parameters become ? in order; PostgreSQL ::casts are left alone")
    void parse() {
        NamedQuery query = NamedQuery.parse("SELECT * FROM t WHERE a = :a AND b >= :from::date AND c = :a");

        assertThat(query.jdbcSql()).isEqualTo("SELECT * FROM t WHERE a = ? AND b >= ?::date AND c = ?");
        assertThat(query.parameters()).containsExactly("a", "from", "a");
    }
}

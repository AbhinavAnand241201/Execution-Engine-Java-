package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.Contexts.context;
import static com.flowforge.runtime.executor.Contexts.json;
import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.executor.TaskResult;
import com.flowforge.runtime.testing.IntegrationTestBase;
import com.flowforge.core.executor.CancellationToken;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;

class DatabaseTaskExecutorIT extends IntegrationTestBase {

    private static DatabaseTaskExecutor executor;

    @BeforeAll
    static void setUp() throws Exception {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setUrl(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        try (Connection connection = dataSource.getConnection(); Statement sql = connection.createStatement()) {
            sql.execute("CREATE SCHEMA IF NOT EXISTS f10_demo");
            sql.execute("DROP TABLE IF EXISTS f10_demo.orders");
            sql.execute("CREATE TABLE f10_demo.orders (id INT PRIMARY KEY, customer TEXT, amount NUMERIC(10,2), "
                + "status TEXT, created_at TIMESTAMPTZ)");
            sql.execute("INSERT INTO f10_demo.orders SELECT g, 'c' || (g % 3), g * 1.50, "
                + "CASE WHEN g % 2 = 0 THEN 'PAID' ELSE 'NEW' END, TIMESTAMPTZ '2026-01-01' + g * INTERVAL '1 day' "
                + "FROM generate_series(1, 10) g");
        }
        executor = new DatabaseTaskExecutor(dataSource, new DatabaseExecutorSettings(Map.of(
            "orders_by_status", "SELECT id, amount, status FROM f10_demo.orders WHERE status = :status ORDER BY id",
            "orders_between_dates", "SELECT id FROM f10_demo.orders WHERE created_at >= :from::timestamptz "
                + "AND created_at < :to::timestamptz ORDER BY id",
            "all_orders", "SELECT id FROM f10_demo.orders ORDER BY id",
            "sneaky_write", "UPDATE f10_demo.orders SET status = 'HACKED' RETURNING id",
            "slow", "SELECT pg_sleep(:seconds)"),
            Duration.ofMillis(500), 5), Contexts.JSON);
    }

    @Test
    @DisplayName("T10.7 a named query with params returns rows and rowCount; rows over max-rows are cut")
    void namedQuery() throws Exception {
        TaskResult result = executor.execute(context(json("{\"query\":\"orders_by_status\",\"params\":{\"status\":\"PAID\"}}")));

        assertThat(result).isInstanceOfSatisfying(TaskResult.Success.class, s -> {
            assertThat(s.output().get("rowCount").asInt()).isEqualTo(5);
            assertThat(s.output().get("rows").get(0).get("id").asInt()).isEqualTo(2);
            assertThat(s.output().get("rows").get(0).get("amount").decimalValue()).isEqualByComparingTo("3.00");
            assertThat(s.output().has("truncated")).isFalse();
        });

        TaskResult dates = executor.execute(context(json(
            "{\"query\":\"orders_between_dates\",\"params\":{\"from\":\"2026-01-03\",\"to\":\"2026-01-05\"}}")));
        assertThat(dates).isInstanceOfSatisfying(TaskResult.Success.class,
            s -> assertThat(s.output().get("rows").findValuesAsText("id")).containsExactly("2", "3"));

        TaskResult capped = executor.execute(context(json("{\"query\":\"all_orders\"}")));
        assertThat(capped).isInstanceOfSatisfying(TaskResult.Success.class, s -> {
            assertThat(s.output().get("rowCount").asInt()).isEqualTo(5);
            assertThat(s.output().get("truncated").asBoolean()).isTrue();
        });
    }

    @Test
    @DisplayName("T10.7 an unknown query, a missing param and an unexpected param are validation errors")
    void validation() {
        assertThat(executor.validateConfig(json("{\"query\":\"drop_everything\"}"))).singleElement()
            .satisfies(e -> assertThat(e.path()).isEqualTo("query"));
        assertThat(executor.validateConfig(json("{\"query\":\"orders_by_status\"}"))).singleElement()
            .satisfies(e -> assertThat(e.path()).isEqualTo("params.status"));
        assertThat(executor.validateConfig(json("{\"query\":\"orders_by_status\",\"params\":{\"status\":\"x\",\"y\":1}}")))
            .singleElement().satisfies(e -> assertThat(e.path()).isEqualTo("params.y"));
        assertThat(executor.validateConfig(json("{\"query\":\"orders_by_status\",\"params\":{\"status\":{\"a\":1}}}")))
            .singleElement().satisfies(e -> assertThat(e.path()).isEqualTo("params.status"));
    }

    @Test
    @DisplayName("T10.7 a write statement configured as a query fails (read-only transaction) and changes nothing")
    void readOnly() throws Exception {
        TaskResult result = executor.execute(context(json("{\"query\":\"sneaky_write\"}")));

        assertThat(result).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.errorCode()).isEqualTo("DATABASE_ERROR");
            assertThat(f.message()).contains("25006");
            assertThat(f.retryable()).isFalse();
        });
        TaskResult after = executor.execute(context(json("{\"query\":\"orders_by_status\",\"params\":{\"status\":\"HACKED\"}}")));
        assertThat(after).isInstanceOfSatisfying(TaskResult.Success.class,
            s -> assertThat(s.output().get("rowCount").asInt()).isZero());
    }

    @Test
    @DisplayName("T10.extra-7 a pooled connection comes back clean: not read-only, auto-commit on, default "
        + "statement_timeout")
    void pooledConnectionIsReset() throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(POSTGRES.getJdbcUrl());
        config.setUsername(POSTGRES.getUsername());
        config.setPassword(POSTGRES.getPassword());
        config.setMaximumPoolSize(1);
        try (HikariDataSource pool = new HikariDataSource(config)) {
            DatabaseTaskExecutor pooled = new DatabaseTaskExecutor(pool, new DatabaseExecutorSettings(
                Map.of("all_orders", "SELECT id FROM f10_demo.orders"), Duration.ofMillis(1_234), 5), Contexts.JSON);
            assertThat(pooled.execute(context(json("{\"query\":\"all_orders\"}")))).isInstanceOf(TaskResult.Success.class);

            try (Connection connection = pool.getConnection(); Statement sql = connection.createStatement();
                 ResultSet timeout = sql.executeQuery("SHOW statement_timeout")) {
                assertThat(connection.isReadOnly()).isFalse();
                assertThat(connection.getAutoCommit()).isTrue();
                timeout.next();
                assertThat(timeout.getString(1)).isEqualTo("0");
            }
        }
    }

    @Test
    @DisplayName("T10.extra-7 a token cancelled before the query starts → Cancelled without running it")
    void cancelledBeforeStart() throws Exception {
        CancellationToken token = new CancellationToken();
        token.cancel();
        long started = System.nanoTime();
        TaskResult result = executor.execute(Contexts.context(java.util.UUID.randomUUID(), 1,
            json("{\"query\":\"slow\",\"params\":{\"seconds\":5}}"), Map.of(), null, token));

        assertThat(result).isInstanceOf(TaskResult.Cancelled.class);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("T10.7 statement_timeout stops pg_sleep: retryable TIMEOUT well before the sleep ends")
    void statementTimeout() throws Exception {
        long started = System.nanoTime();
        TaskResult result = executor.execute(context(json("{\"query\":\"slow\",\"params\":{\"seconds\":5}}")));

        assertThat(result).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.errorCode()).isEqualTo("TIMEOUT");
            assertThat(f.retryable()).isTrue();
        });
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
    }
}

package com.flowforge.orchestrator.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flowforge.orchestrator.FlowForgeOrchestratorApplication;
import com.flowforge.runtime.testing.IntegrationTestBase;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Starts the whole application more than once against the shared database, like restarts in production. */
class AdminSeederIT extends IntegrationTestBase {

    @Test
    @DisplayName("T07.7 the admin is seeded once across two application restarts")
    void seededOnceAcrossRestarts() throws Exception {
        String adminEmail = "seed-" + UUID.randomUUID() + "@example.com";

        String first;
        try (ConfigurableApplicationContext ignored = start("--flowforge.security.admin.email=" + adminEmail,
            "--flowforge.security.admin.password=seed-admin-password")) {
            first = adminRow(adminEmail);
        }
        try (ConfigurableApplicationContext ignored = start("--flowforge.security.admin.email=" + adminEmail,
            "--flowforge.security.admin.password=a-different-password-on-restart")) {
            // Same id, same password hash, same created_at: the second start left the existing account alone.
            assertThat(adminRow(adminEmail)).isEqualTo(first);
        }
    }

    @Test
    @DisplayName("T07.8 a JWT secret shorter than 32 bytes stops the application from starting, with a clear message")
    void shortSecretFailsStartup() {
        assertThatThrownBy(() -> start("--flowforge.security.jwt.secret=too-short"))
            .hasRootCauseMessage("flowforge.security.jwt.secret (env FLOWFORGE_JWT_SECRET) is required and must be "
                + "at least 32 bytes; got 9");
    }

    /** Command-line arguments, because they override application.yml (builder "properties" are only defaults). */
    private static ConfigurableApplicationContext start(String... extraArguments) {
        List<String> arguments = new ArrayList<>(List.of("--server.port=0",
            "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
            "--spring.datasource.username=" + POSTGRES.getUsername(),
            "--spring.datasource.password=" + POSTGRES.getPassword(),
            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
            "--spring.data.redis.host=" + REDIS.getHost(),
            "--spring.data.redis.port=" + REDIS.getMappedPort(6379)));
        arguments.addAll(List.of(extraArguments));
        return new SpringApplicationBuilder(FlowForgeOrchestratorApplication.class).profiles("test")
            .run(arguments.toArray(String[]::new));
    }

    /** The one ADMIN row for this email as "id|hash|created_at"; fails if there isn't exactly one. */
    private static String adminRow(String email) throws Exception {
        try (Connection connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(),
                POSTGRES.getPassword());
             PreparedStatement query = connection.prepareStatement(
                 "SELECT id, password_hash, created_at FROM users WHERE email = ? AND role = 'ADMIN'")) {
            query.setString(1, email);
            try (ResultSet result = query.executeQuery()) {
                assertThat(result.next()).as("an ADMIN row").isTrue();
                String row = result.getString(1) + "|" + result.getString(2) + "|" + result.getString(3);
                assertThat(result.next()).as("only one ADMIN row").isFalse();
                return row;
            }
        }
    }

    @Test
    @DisplayName("T07.extra-2 a seeded admin password that registration would reject stops startup, naming the env var")
    void weakSeedPasswordFailsStartup() {
        assertThatThrownBy(() -> start("--flowforge.security.admin.email=weak-" + UUID.randomUUID() + "@example.com",
            "--flowforge.security.admin.password=change-me"))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("FLOWFORGE_ADMIN_PASSWORD: Password must be at least 10 characters");
    }
}

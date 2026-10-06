package com.flowforge.orchestrator.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.flowforge.orchestrator.application.AuthService;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
class AdminSeederTest {

    @Test
    @DisplayName("T07.7 missing admin env → seeding skipped with a warning, nothing written")
    void missingEnvSkipsWithWarning(CapturedOutput output) {
        AuthService accounts = mock(AuthService.class);
        SecurityProperties properties = new SecurityProperties(
            new SecurityProperties.Jwt("x".repeat(32), Duration.ofHours(1), "flowforge"),
            new SecurityProperties.Account(null, null), new SecurityProperties.Account("", ""),
            new SecurityProperties.Cors(List.of()));

        new AdminSeeder(accounts, properties).run(new DefaultApplicationArguments());

        verifyNoInteractions(accounts);
        assertThat(output.getOut()).contains("ADMIN account not seeded").contains("FLOWFORGE_ADMIN_EMAIL");
    }
}

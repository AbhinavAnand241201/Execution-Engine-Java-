package com.flowforge.orchestrator.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.orchestrator.infrastructure.persistence.UserRepository;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

class RegistrationRaceIT extends OrchestratorIntegrationTestBase {

    @Autowired
    private UserRepository users;

    @Test
    @DisplayName("T07.extra-3 four concurrent registrations of one email (mixed case), 50 times: exactly one 201, the "
        + "rest EMAIL_TAKEN, never a 500, one row")
    void concurrentRegistrationHasOneWinner() throws Exception {
        // Real repository and database; a cheap BCrypt cost so 200 registrations finish quickly.
        AuthService auth = new AuthService(users, new BCryptPasswordEncoder(4), mock(TokenIssuer.class), Clock.systemUTC());
        String[] emails = new String[1];

        List<String> roundEmails = new ArrayList<>();
        List<List<Object>> rounds = ConcurrencyHarness.race(4, 50, thread -> {
            String email = thread % 2 == 0 ? emails[0] : emails[0].toUpperCase(Locale.ROOT);
            return auth.register(email, "correct-horse-battery");
        }, () -> {
            emails[0] = "race-" + UUID.randomUUID() + "@example.com";
            roundEmails.add(emails[0]);
        });

        for (List<Object> outcomes : rounds) {
            assertThat(outcomes).filteredOn(UserView.class::isInstance).hasSize(1);
            assertThat(outcomes).filteredOn(o -> !(o instanceof UserView)).hasSize(3).allSatisfy(o ->
                assertThat(o).isInstanceOfSatisfying(ApplicationException.class, e ->
                    assertThat(e.errorCode()).isEqualTo(ErrorCode.EMAIL_TAKEN)));
        }
        assertThat(rounds).hasSize(50);
        for (String email : roundEmails) {
            assertThat(users.findByEmail(email)).as("exactly one stored row for %s", email).isPresent();
        }
    }
}

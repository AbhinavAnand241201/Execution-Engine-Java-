package com.flowforge.orchestrator.config;

import com.flowforge.orchestrator.application.ApplicationException;
import com.flowforge.orchestrator.application.AuthService;
import com.flowforge.orchestrator.application.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Creates the ADMIN (and optional OPERATOR) account from the environment at startup, because nobody can register
 * as ADMIN through the API. Idempotent: an existing account is left alone, so restarts and several instances are
 * safe. Never logs the password.
 */
@Component
public class AdminSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AdminSeeder.class);

    private final AuthService accounts;
    private final SecurityProperties properties;

    public AdminSeeder(AuthService accounts, SecurityProperties properties) {
        this.accounts = accounts;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        seed(Role.ADMIN, properties.admin(), "FLOWFORGE_ADMIN");
        if (properties.operator().isConfigured()) {
            seed(Role.OPERATOR, properties.operator(), "FLOWFORGE_OPERATOR");
        }
    }

    private void seed(Role role, SecurityProperties.Account account, String envPrefix) {
        if (!account.isConfigured()) {
            log.warn("{} account not seeded: set {}_EMAIL / {}_PASSWORD to create one", role, envPrefix, envPrefix);
            return;
        }
        AuthService.SeedResult result;
        try {
            result = accounts.ensureAccount(account.email(), account.password(), role);
        } catch (ApplicationException weakPassword) {
            // Refuse to start rather than create a privileged account with a password registration would reject.
            throw new IllegalStateException(envPrefix + "_PASSWORD: " + weakPassword.getMessage());
        }
        switch (result) {
            case CREATED -> log.info("{} account created email={}", role, account.email());
            case PRESENT -> log.info("{} account already present email={}", role, account.email());
            case PRESENT_WITH_OTHER_ROLE -> log.warn("{} account NOT seeded: email={} already exists with another role; "
                + "grant the role via PATCH /api/v1/admin/users/<id>/role or SQL", role, account.email());
        }
    }
}

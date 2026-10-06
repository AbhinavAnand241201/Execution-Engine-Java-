package com.flowforge.orchestrator.application;

import java.util.UUID;

/**
 * Issues access tokens (a port). The application decides <em>who</em> gets a token; how it is signed is an
 * infrastructure detail, implemented by {@code JwtTokenIssuer}.
 */
public interface TokenIssuer {

    AccessToken issue(UUID userId, String email, Role role);
}

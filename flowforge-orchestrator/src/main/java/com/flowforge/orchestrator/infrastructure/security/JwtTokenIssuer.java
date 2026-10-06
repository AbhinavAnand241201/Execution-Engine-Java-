package com.flowforge.orchestrator.infrastructure.security;

import com.flowforge.orchestrator.application.AccessToken;
import com.flowforge.orchestrator.application.Role;
import com.flowforge.orchestrator.application.TokenIssuer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;

/**
 * Signs HS256 JWTs with claims {@code sub} (user id), {@code email}, {@code roles}, {@code iss}, {@code iat} and
 * {@code exp}. The token carries everything authorization needs, so requests don't hit the database to check roles.
 */
public class JwtTokenIssuer implements TokenIssuer {

    private final JwtEncoder encoder;
    private final Duration ttl;
    private final String issuer;
    private final Clock clock;

    public JwtTokenIssuer(JwtEncoder encoder, Duration ttl, String issuer, Clock clock) {
        this.encoder = encoder;
        this.ttl = ttl;
        this.issuer = issuer;
        this.clock = clock;
    }

    @Override
    public AccessToken issue(UUID userId, String email, Role role) {
        Instant now = clock.instant();
        JwtClaimsSet claims = JwtClaimsSet.builder()
            .issuer(issuer)
            .subject(userId.toString())
            .issuedAt(now)
            .expiresAt(now.plus(ttl))
            .claim("email", email)
            .claim("roles", List.of(role.name()))
            .build();
        String token = encoder.encode(JwtEncoderParameters.from(JwsHeader.with(MacAlgorithm.HS256).build(), claims))
            .getTokenValue();
        return new AccessToken(token, "Bearer", ttl.toSeconds());
    }
}

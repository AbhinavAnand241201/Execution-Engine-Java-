package com.flowforge.orchestrator.config;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code flowforge.security.*}. The JWT secret is checked while the properties are bound, so a missing or
 * short secret stops startup with a message that names the environment variable, instead of failing on first login.
 */
@ConfigurationProperties("flowforge.security")
public record SecurityProperties(Jwt jwt, Account admin, Account operator, Cors cors) {

    public SecurityProperties {
        admin = admin == null ? new Account(null, null) : admin;
        operator = operator == null ? new Account(null, null) : operator;
        cors = cors == null ? new Cors(List.of()) : cors;
    }

    /**
     * HS256 needs a key of at least 256 bits; anything shorter is guessable offline from one captured token. The secret
     * is used as text: an {@code openssl rand -base64 48} value is not decoded, its 64 characters are the key bytes.
     */
    public record Jwt(String secret, Duration ttl, String issuer) {

        public static final int MIN_SECRET_BYTES = 32;

        public Jwt {
            if (ttl == null || issuer == null || issuer.isBlank()) {
                throw new IllegalStateException("flowforge.security.jwt.ttl and .issuer must be set (defaults are in "
                    + "application.yml)");
            }
            int length = secret == null ? 0 : secret.getBytes(StandardCharsets.UTF_8).length;
            if (length < MIN_SECRET_BYTES) {
                throw new IllegalStateException("flowforge.security.jwt.secret (env FLOWFORGE_JWT_SECRET) is required and "
                    + "must be at least " + MIN_SECRET_BYTES + " bytes; got " + length);
            }
        }

        public byte[] secretBytes() {
            return secret.getBytes(StandardCharsets.UTF_8);
        }
    }

    /** An account seeded at startup; both values empty means "don't seed". */
    public record Account(String email, String password) {

        public boolean isConfigured() {
            return email != null && !email.isBlank() && password != null && !password.isBlank();
        }
    }

    /** Browser origins allowed to call the API; empty = no cross-origin access. */
    public record Cors(List<String> allowedOrigins) {

        public Cors {
            allowedOrigins = allowedOrigins == null ? List.of() : List.copyOf(allowedOrigins);
        }
    }
}

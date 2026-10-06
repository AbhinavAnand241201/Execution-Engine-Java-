package com.flowforge.orchestrator.api.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwsHeader;
import org.springframework.security.oauth2.jwt.JwtClaimsSet;
import org.springframework.security.oauth2.jwt.JwtEncoderParameters;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import com.nimbusds.jose.jwk.source.ImmutableSecret;

class AuthIT extends OrchestratorIntegrationTestBase {

    private static final String TEST_SECRET = "test-only-jwt-secret-do-not-use-anywhere-else-0123456789-padded-to-64b";

    @Test
    @DisplayName("T07.1 register → 201 USER; duplicate email in any case → 409 EMAIL_TAKEN; weak or > 72-byte "
        + "password → 400 WEAK_PASSWORD")
    void register() {
        ApiClient api = api();
        String email = ApiClient.uniqueEmail();

        ResponseEntity<String> created = api.register(email, ApiClient.PASSWORD);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        JsonNode user = api.read(created);
        assertThat(user.get("email").asText()).isEqualTo(email);
        assertThat(user.get("role").asText()).isEqualTo("USER");
        assertThat(UUID.fromString(user.get("id").asText())).isNotNull();

        ResponseEntity<String> duplicate = api.register(email.toUpperCase(), ApiClient.PASSWORD);
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api.read(duplicate).get("error").asText()).isEqualTo("EMAIL_TAKEN");

        ResponseEntity<String> weak = api.register(ApiClient.uniqueEmail(), "short");
        assertThat(weak.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(api.read(weak).get("error").asText()).isEqualTo("WEAK_PASSWORD");

        ResponseEntity<String> tooLongForBcrypt = api.register(ApiClient.uniqueEmail(), "p".repeat(73));
        assertThat(tooLongForBcrypt.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(api.read(tooLongForBcrypt).get("error").asText()).isEqualTo("WEAK_PASSWORD");

        ResponseEntity<String> notAnEmail = api.register("not-an-email", ApiClient.PASSWORD);
        assertThat(api.read(notAnEmail).get("error").asText()).isEqualTo("VALIDATION_FAILED");
    }

    @Test
    @DisplayName("T07.2 login → JWT with sub, email, roles, iss, iat, exp (1 h); wrong password and unknown email → identical 401")
    void login() throws Exception {
        ApiClient api = api();
        String email = ApiClient.uniqueEmail();
        String userId = api.read(api.register(email, ApiClient.PASSWORD)).get("id").asText();

        JsonNode token = api.read(api.login(email.toUpperCase(), ApiClient.PASSWORD));
        assertThat(token.get("tokenType").asText()).isEqualTo("Bearer");
        assertThat(token.get("expiresIn").asLong()).isEqualTo(3600);

        JsonNode claims = json.readTree(Base64.getUrlDecoder().decode(token.get("accessToken").asText().split("\\.")[1]));
        assertThat(claims.get("sub").asText()).isEqualTo(userId);
        assertThat(claims.get("email").asText()).isEqualTo(email);
        assertThat(claims.get("roles")).hasSize(1);
        assertThat(claims.get("roles").get(0).asText()).isEqualTo("USER");
        assertThat(claims.get("iss").asText()).isEqualTo("flowforge");
        assertThat(claims.get("exp").asLong() - claims.get("iat").asLong()).isEqualTo(3600);

        ResponseEntity<String> wrongPassword = api.login(email, "wrong-password-123");
        ResponseEntity<String> unknownEmail = api.login(ApiClient.uniqueEmail(), "wrong-password-123");
        assertThat(wrongPassword.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(unknownEmail.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        JsonNode first = api.read(wrongPassword);
        JsonNode second = api.read(unknownEmail);
        assertThat(first.get("error")).isEqualTo(second.get("error"));
        assertThat(first.get("message")).isEqualTo(second.get("message"));
    }

    @Test
    @DisplayName("T07.3 no token, expired, tampered (payload escalated to ADMIN, signature flipped), other secret, "
        + "alg none, HS512 → 401")
    void badTokens() {
        ApiClient api = api();
        String valid = api.newUserToken();
        assertThat(api.get("/api/v1/auth/me", valid).getStatusCode()).isEqualTo(HttpStatus.OK);

        String expired = sign(TEST_SECRET, MacAlgorithm.HS256, Instant.now().minusSeconds(3 * 3600),
            Instant.now().minusSeconds(2 * 3600));
        String otherSecret = sign("a-completely-different-secret-of-sufficient-length!!", MacAlgorithm.HS256, Instant.now(),
            Instant.now().plusSeconds(3600));
        // Same key, different algorithm: rejected only because the decoder pins HS256.
        String hs512 = sign(TEST_SECRET, MacAlgorithm.HS512, Instant.now(), Instant.now().plusSeconds(3600));

        String[] parts = valid.split("\\.");
        // Payload swapped for one claiming ADMIN, original signature kept: a privilege-escalation attempt.
        String escalatedPayload = base64Url(new String(Base64.getUrlDecoder().decode(parts[1]), StandardCharsets.UTF_8)
            .replace("\"USER\"", "\"ADMIN\""));
        String escalated = parts[0] + "." + escalatedPayload + "." + parts[2];
        // A middle signature character changes real signature bits (the last one may only carry padding bits).
        int middle = parts[2].length() / 2;
        String flipped = parts[0] + "." + parts[1] + "." + parts[2].substring(0, middle)
            + (parts[2].charAt(middle) == 'A' ? 'B' : 'A') + parts[2].substring(middle + 1);
        String unsigned = base64Url("{\"alg\":\"none\"}") + "." + parts[1] + ".";

        for (String token : new String[] {null, expired, otherSecret, hs512, escalated, flipped, unsigned, "not.a.jwt"}) {
            ResponseEntity<String> response = api.get("/api/v1/auth/me", token);
            assertThat(response.getStatusCode()).as("token %s", token).isEqualTo(HttpStatus.UNAUTHORIZED);
            JsonNode body = api.read(response);
            assertThat(body.get("error").asText()).isEqualTo("UNAUTHORIZED");
            assertThat(body.get("correlationId").asText()).isNotBlank();
        }
    }

    @Test
    @DisplayName("T07.6 the password hash never appears in any auth response")
    void passwordHashNeverReturned() {
        ApiClient api = api();
        String email = ApiClient.uniqueEmail();
        ResponseEntity<String> registered = api.register(email, ApiClient.PASSWORD);
        String userId = api.read(registered).get("id").asText();
        ResponseEntity<String> loggedIn = api.login(email, ApiClient.PASSWORD);
        String token = api.read(loggedIn).get("accessToken").asText();
        ResponseEntity<String> me = api.get("/api/v1/auth/me", token);
        ResponseEntity<String> roleChange = api.send(HttpMethod.PATCH, "/api/v1/admin/users/" + userId + "/role",
            Map.of("role", "OPERATOR"), api.adminToken());

        assertThat(roleChange.getStatusCode()).isEqualTo(HttpStatus.OK);
        for (ResponseEntity<String> response : List.of(registered, loggedIn, me, roleChange)) {
            assertThat(response.getBody()).doesNotContainIgnoringCase("password").doesNotContain("$2a$", "$2b$", "$2y$");
        }
        // The access token is a JWT; its payload must not carry the hash either.
        String payload = new String(Base64.getUrlDecoder().decode(token.split("\\.")[1]), StandardCharsets.UTF_8);
        assertThat(payload).doesNotContain("$2").doesNotContainIgnoringCase("password");
    }

    private static String sign(String secret, MacAlgorithm algorithm, Instant issuedAt, Instant expiresAt) {
        NimbusJwtEncoder encoder = new NimbusJwtEncoder(
            new ImmutableSecret<>(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), algorithm.getName())));
        JwtClaimsSet claims = JwtClaimsSet.builder().issuer("flowforge").subject(UUID.randomUUID().toString())
            .issuedAt(issuedAt).expiresAt(expiresAt).claim("roles", List.of("USER")).claim("email", "x@example.com").build();
        return encoder.encode(JwtEncoderParameters.from(JwsHeader.with(algorithm).build(), claims)).getTokenValue();
    }

    private static String base64Url(String text) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(text.getBytes(StandardCharsets.UTF_8));
    }
}

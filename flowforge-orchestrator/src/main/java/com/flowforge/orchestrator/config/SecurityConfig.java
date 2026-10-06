package com.flowforge.orchestrator.config;

import com.flowforge.orchestrator.api.SecurityErrorResponses;
import com.flowforge.orchestrator.application.TokenIssuer;
import com.flowforge.orchestrator.infrastructure.security.JwtTokenIssuer;
import com.nimbusds.jose.jwk.source.ImmutableSecret;
import java.time.Clock;
import java.time.Duration;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.access.hierarchicalroles.RoleHierarchy;
import org.springframework.security.access.hierarchicalroles.RoleHierarchyImpl;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jose.jws.MacAlgorithm;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtEncoder;
import org.springframework.security.oauth2.jwt.JwtIssuerValidator;
import org.springframework.security.oauth2.jwt.JwtTimestampValidator;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.NimbusJwtEncoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

/**
 * Stateless bearer-token security. There is no session and no cookie, so CSRF protection is off: CSRF works by making
 * a browser send cookies it already holds, and a bearer token is never sent automatically. (If the API ever moved
 * the token into a cookie, CSRF protection would have to come back.)
 *
 * <p>A request that carries a bearer token is authenticated even on public URLs, so a stale token sent to
 * {@code /auth/login} gets a 401: clients should drop an expired token before logging in again.
 */
@Configuration
@EnableMethodSecurity
public class SecurityConfig {

    /** Cost 12 ≈ a few hundred ms per hash: slow for an attacker guessing offline, still fine for a login. */
    static final int BCRYPT_STRENGTH = 12;

    @Bean
    SecurityFilterChain securityFilterChain(HttpSecurity http, SecurityErrorResponses errors,
                                            JwtAuthenticationConverter jwtConverter) throws Exception {
        return http
            .csrf(AbstractHttpConfigurer::disable)
            .cors(Customizer.withDefaults())
            .httpBasic(AbstractHttpConfigurer::disable)
            .formLogin(AbstractHttpConfigurer::disable)
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                .requestMatchers(HttpMethod.POST, "/api/v1/auth/register", "/api/v1/auth/login").permitAll()
                .requestMatchers("/api/v1/system/info", "/error", "/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html",
                    "/actuator/health", "/actuator/health/**").permitAll()
                .requestMatchers("/actuator/**").hasRole("ADMIN")
                .requestMatchers("/api/v1/admin/**").hasRole("ADMIN")
                .requestMatchers("/api/v1/workers/**", "/api/v1/dead-letter/**").hasRole("OPERATOR")
                .anyRequest().authenticated())
            .oauth2ResourceServer(oauth -> oauth
                .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtConverter))
                .authenticationEntryPoint(errors))
            .exceptionHandling(handling -> handling.authenticationEntryPoint(errors).accessDeniedHandler(errors))
            .build();
    }

    /** ADMIN can do everything OPERATOR can, who can do everything USER can. */
    @Bean
    static RoleHierarchy roleHierarchy() {
        return RoleHierarchyImpl.fromHierarchy("ROLE_ADMIN > ROLE_OPERATOR\nROLE_OPERATOR > ROLE_USER");
    }

    /** The token's {@code roles} claim becomes {@code ROLE_*} authorities; the user id ({@code sub}) is the name. */
    @Bean
    JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtGrantedAuthoritiesConverter authorities = new JwtGrantedAuthoritiesConverter();
        authorities.setAuthoritiesClaimName("roles");
        authorities.setAuthorityPrefix("ROLE_");
        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(authorities);
        return converter;
    }

    @Bean
    PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(BCRYPT_STRENGTH);
    }

    @Bean
    SecretKey jwtKey(SecurityProperties properties) {
        return new SecretKeySpec(properties.jwt().secretBytes(), "HmacSHA256");
    }

    @Bean
    JwtEncoder jwtEncoder(SecretKey jwtKey) {
        return new NimbusJwtEncoder(new ImmutableSecret<>(jwtKey));
    }

    /** Accepts only HS256 tokens signed with our key, from our issuer, and not expired (by the injected clock). */
    @Bean
    JwtDecoder jwtDecoder(SecretKey jwtKey, SecurityProperties properties, Clock clock) {
        NimbusJwtDecoder decoder = NimbusJwtDecoder.withSecretKey(jwtKey).macAlgorithm(MacAlgorithm.HS256).build();
        // Up to 60 s of clock difference between issuer and validator is tolerated (an expired token stays valid that
        // much longer); one app issues and validates here, so this only covers clock adjustments.
        JwtTimestampValidator timestamps = new JwtTimestampValidator(Duration.ofSeconds(60));
        timestamps.setClock(clock);
        decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(timestamps,
            new JwtIssuerValidator(properties.jwt().issuer())));
        return decoder;
    }

    @Bean
    TokenIssuer tokenIssuer(JwtEncoder jwtEncoder, SecurityProperties properties, Clock clock) {
        return new JwtTokenIssuer(jwtEncoder, properties.jwt().ttl(), properties.jwt().issuer(), clock);
    }

    /** Browser origins come from config; the default (none) means only non-browser or same-origin clients. */
    @Bean
    CorsConfigurationSource corsConfigurationSource(SecurityProperties properties) {
        CorsConfiguration cors = new CorsConfiguration();
        cors.setAllowedOrigins(properties.cors().allowedOrigins());
        cors.addAllowedMethod("*");
        cors.addAllowedHeader("*");
        cors.addExposedHeader("X-Correlation-Id");
        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", cors);
        return source;
    }
}

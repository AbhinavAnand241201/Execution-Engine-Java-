package com.flowforge.orchestrator.application;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.orchestrator.infrastructure.persistence.UserEntity;
import com.flowforge.orchestrator.infrastructure.persistence.UserRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Locale;
import java.util.UUID;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

/**
 * Accounts and logins. Methods are deliberately not one big transaction: BCrypt takes a few hundred milliseconds,
 * and holding a database connection while hashing would cap throughput at the pool size. Each repository call is
 * its own short transaction; the unique constraint settles races.
 */
@Service
public class AuthService {

    static final int MIN_PASSWORD_LENGTH = 10;
    /** BCrypt only uses the first 72 bytes and Spring's encoder rejects longer input outright. */
    static final int MAX_PASSWORD_BYTES = 72;
    private static final String INVALID_LOGIN = "Invalid email or password";

    /** Outcome of {@link #ensureAccount}: created, already there, or the email exists with a different role. */
    public enum SeedResult { CREATED, PRESENT, PRESENT_WITH_OTHER_ROLE }

    private final UserRepository users;
    private final PasswordEncoder passwords;
    private final TokenIssuer tokens;
    private final Clock clock;
    /** Checked against when the email is unknown, so "no such user" takes as long as "wrong password". */
    private final String dummyHash;

    public AuthService(UserRepository users, PasswordEncoder passwords, TokenIssuer tokens, Clock clock) {
        this.users = users;
        this.passwords = passwords;
        this.tokens = tokens;
        this.clock = clock;
        this.dummyHash = passwords.encode("dummy-password-used-only-for-timing");
    }

    public UserView register(String email, String password) {
        checkPasswordPolicy(password);
        String normalized = normalize(email);
        if (users.existsByEmail(normalized)) {
            throw emailTaken();
        }
        UserEntity user = new UserEntity(UUID.randomUUID(), normalized, passwords.encode(password), Role.USER.name(),
            clock.instant());
        try {
            return view(users.saveAndFlush(user));
        } catch (DataIntegrityViolationException concurrentRegistration) {
            throw emailTaken(); // another request registered the same address between the check and the insert
        }
    }

    /** Same 401 and message for unknown email and wrong password, so the endpoint can't be used to find accounts. */
    public AccessToken login(String email, String password) {
        UserEntity user = users.findByEmail(normalize(email)).orElse(null);
        if (user == null) {
            passwords.matches(password, dummyHash);
            throw new ApplicationException(ErrorCode.UNAUTHORIZED, INVALID_LOGIN);
        }
        if (!passwords.matches(password, user.getPasswordHash())) {
            throw new ApplicationException(ErrorCode.UNAUTHORIZED, INVALID_LOGIN);
        }
        return tokens.issue(user.getId(), user.getEmail(), Role.valueOf(user.getRole()));
    }

    public UserView me(AuthenticatedUser caller) {
        return users.findById(caller.id()).map(AuthService::view).orElseThrow(() -> new NotFoundException("user", caller.id()));
    }

    /**
     * Creates an account with the given role unless the email already exists; an existing account is never changed.
     * The password must pass the same policy as registration (a seeded ADMIN must not be the weakest account).
     */
    public SeedResult ensureAccount(String email, String password, Role role) {
        checkPasswordPolicy(password);
        String normalized = normalize(email);
        UserEntity existing = users.findByEmail(normalized).orElse(null);
        if (existing != null) {
            return existing.getRole().equals(role.name()) ? SeedResult.PRESENT : SeedResult.PRESENT_WITH_OTHER_ROLE;
        }
        try {
            users.saveAndFlush(new UserEntity(UUID.randomUUID(), normalized, passwords.encode(password), role.name(),
                clock.instant()));
            return SeedResult.CREATED;
        } catch (DataIntegrityViolationException createdConcurrently) {
            return SeedResult.PRESENT; // another instance seeded it at the same time
        }
    }

    private static void checkPasswordPolicy(String password) {
        if (password.length() < MIN_PASSWORD_LENGTH) {
            throw new ApplicationException(ErrorCode.WEAK_PASSWORD,
                "Password must be at least " + MIN_PASSWORD_LENGTH + " characters");
        }
        if (password.getBytes(StandardCharsets.UTF_8).length > MAX_PASSWORD_BYTES) {
            throw new ApplicationException(ErrorCode.WEAK_PASSWORD,
                "Password must be at most " + MAX_PASSWORD_BYTES + " bytes (a BCrypt limit)");
        }
    }

    static UserView view(UserEntity user) {
        return new UserView(user.getId(), user.getEmail(), Role.valueOf(user.getRole()), user.getCreatedAt());
    }

    private static String normalize(String email) {
        return email.trim().toLowerCase(Locale.ROOT);
    }

    private static ApplicationException emailTaken() {
        return new ApplicationException(ErrorCode.EMAIL_TAKEN, "An account with this email already exists");
    }
}

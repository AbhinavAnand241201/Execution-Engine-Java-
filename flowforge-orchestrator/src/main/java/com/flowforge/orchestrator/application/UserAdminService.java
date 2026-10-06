package com.flowforge.orchestrator.application;

import com.flowforge.orchestrator.infrastructure.persistence.UserEntity;
import com.flowforge.orchestrator.infrastructure.persistence.UserRepository;
import java.time.Clock;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Administrative changes to accounts. A role change takes effect at the user's next login: roles travel inside the
 * (stateless) token, which stays valid until it expires.
 */
@Service
public class UserAdminService {

    private final UserRepository users;
    private final Clock clock;

    public UserAdminService(UserRepository users, Clock clock) {
        this.users = users;
        this.clock = clock;
    }

    @Transactional
    public UserView changeRole(UUID userId, Role role) {
        UserEntity user = users.findById(userId).orElseThrow(() -> new NotFoundException("user", userId));
        user.changeRole(role.name(), clock.instant());
        return AuthService.view(user);
    }
}

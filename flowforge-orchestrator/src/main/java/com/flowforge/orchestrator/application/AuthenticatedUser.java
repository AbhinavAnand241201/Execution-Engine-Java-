package com.flowforge.orchestrator.application;

import java.util.UUID;

/**
 * Who is calling, as stated by a verified token. Services take this instead of an HTTP or Spring Security type, so
 * ownership rules ("only your own workflows") are written once, in the application layer.
 */
public record AuthenticatedUser(UUID id, String email, Role role) {

    /** OPERATOR and ADMIN may read every user's executions. */
    public boolean canReadAll() {
        return role.includes(Role.OPERATOR);
    }
}

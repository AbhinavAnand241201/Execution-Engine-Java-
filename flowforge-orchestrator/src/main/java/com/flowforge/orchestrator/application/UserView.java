package com.flowforge.orchestrator.application;

import java.time.Instant;
import java.util.UUID;

/** A user as the API shows it. There is deliberately no field that could carry the password hash. */
public record UserView(UUID id, String email, Role role, Instant createdAt) {
}

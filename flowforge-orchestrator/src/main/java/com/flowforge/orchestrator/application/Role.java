package com.flowforge.orchestrator.application;

/**
 * What a user may do, in increasing order of power: each role includes everything the roles before it may do. The
 * URL rules use the same order through {@code SecurityConfig.roleHierarchy()}; keep the two in step.
 */
public enum Role {
    USER,
    OPERATOR,
    ADMIN;

    /** Whether this role may do everything {@code other} may (declaration order is the hierarchy). */
    public boolean includes(Role other) {
        return ordinal() >= other.ordinal();
    }
}

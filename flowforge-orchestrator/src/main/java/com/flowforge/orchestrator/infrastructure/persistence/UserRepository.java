package com.flowforge.orchestrator.infrastructure.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/** CRUD for accounts (JPA is used for aggregate CRUD; engine state uses explicit SQL). */
public interface UserRepository extends JpaRepository<UserEntity, UUID> {

    /** {@code email} must already be lower-case. */
    Optional<UserEntity> findByEmail(String email);

    boolean existsByEmail(String email);
}

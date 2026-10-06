package com.flowforge.orchestrator.infrastructure.persistence;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Workflows, always looked up together with their owner so another user's workflow simply isn't found. */
public interface WorkflowRepository extends JpaRepository<WorkflowEntity, UUID>, JpaSpecificationExecutor<WorkflowEntity> {

    Optional<WorkflowEntity> findByIdAndOwnerId(UUID id, UUID ownerId);

    /** Ownership check that loads no entity, so a later locked read is the first (and fresh) read. */
    boolean existsByIdAndOwnerId(UUID id, UUID ownerId);

    boolean existsByOwnerIdAndName(UUID ownerId, String name);

    /**
     * {@code SELECT ... FOR UPDATE}: serializes structural changes to one workflow (creating its next draft,
     * publishing) so the version counters can't be raced.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select w from WorkflowEntity w where w.id = :id and w.ownerId = :ownerId")
    Optional<WorkflowEntity> findForUpdate(@Param("id") UUID id, @Param("ownerId") UUID ownerId);
}

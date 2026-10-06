package com.flowforge.orchestrator.infrastructure.persistence;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** Versions of a workflow; the caller has already checked that the workflow belongs to the requester. */
public interface WorkflowVersionRepository extends JpaRepository<WorkflowVersionEntity, UUID> {

    List<WorkflowVersionEntity> findByWorkflowIdOrderByVersion(UUID workflowId);

    Optional<WorkflowVersionEntity> findByWorkflowIdAndVersion(UUID workflowId, int version);

    Optional<WorkflowVersionEntity> findByWorkflowIdAndStatus(UUID workflowId, String status);

    /**
     * Inserts a draft with a chosen starting {@code lock_version}. Native SQL because JPA always starts a new entity's
     * {@code @Version} at 0, and lock versions must keep increasing across all versions of a workflow (so a token read
     * from one version can never match another).
     */
    @Modifying(flushAutomatically = true) // pending entity changes (e.g. the new workflow row) go first
    @Query(value = "INSERT INTO workflow_versions (id, workflow_id, version, status, definition, lock_version, created_at) "
        + "VALUES (:id, :workflowId, :version, 'DRAFT', CAST(:definition AS jsonb), :lockVersion, :createdAt)",
        nativeQuery = true)
    void insertDraft(@Param("id") UUID id, @Param("workflowId") UUID workflowId, @Param("version") int version,
                     @Param("definition") String definition, @Param("lockVersion") long lockVersion,
                     @Param("createdAt") Instant createdAt);
}

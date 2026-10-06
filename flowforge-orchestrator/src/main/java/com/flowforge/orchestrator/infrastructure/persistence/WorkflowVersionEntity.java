package com.flowforge.orchestrator.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * Row of {@code workflow_versions}. {@code lockVersion} is JPA's optimistic lock: every update runs
 * {@code ... WHERE lock_version = :read}, so of two editors who read the same draft, the second to save gets a
 * conflict instead of silently overwriting the first. Rows are inserted by
 * {@link WorkflowVersionRepository#insertDraft} and only ever updated through this entity.
 */
@Entity
@Table(name = "workflow_versions")
public class WorkflowVersionEntity {

    public static final String DRAFT = "DRAFT";
    public static final String PUBLISHED = "PUBLISHED";

    @Id
    private UUID id;

    @Column(name = "workflow_id", nullable = false, updatable = false)
    private UUID workflowId;

    @Column(nullable = false, updatable = false)
    private int version;

    @Column(nullable = false, length = 20)
    private String status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String definition;

    @Version
    @Column(name = "lock_version", nullable = false)
    private long lockVersion;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected WorkflowVersionEntity() {
        // for JPA
    }

    public void replaceDefinition(String newDefinition) {
        requireDraft();
        this.definition = newDefinition;
    }

    public void publish(Instant now) {
        requireDraft();
        this.status = PUBLISHED;
        this.publishedAt = now;
    }

    public boolean isDraft() {
        return DRAFT.equals(status);
    }

    private void requireDraft() {
        if (!isDraft()) {
            throw new IllegalStateException("version " + id + " is published and immutable");
        }
    }

    public UUID getId() {
        return id;
    }

    public UUID getWorkflowId() {
        return workflowId;
    }

    public int getVersion() {
        return version;
    }

    public String getStatus() {
        return status;
    }

    public String getDefinition() {
        return definition;
    }

    public long getLockVersion() {
        return lockVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}

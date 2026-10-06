package com.flowforge.orchestrator.infrastructure.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** Row of {@code workflows}: a named, owned container of versions with pointers to its latest ones. */
@Entity
@Table(name = "workflows")
public class WorkflowEntity {

    @Id
    private UUID id;

    @Column(name = "owner_id", nullable = false)
    private UUID ownerId;

    @Column(nullable = false, length = 100)
    private String name;

    @Column(length = 1000)
    private String description;

    @Column(name = "latest_version", nullable = false)
    private int latestVersion;

    @Column(name = "latest_published_version")
    private Integer latestPublishedVersion;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected WorkflowEntity() {
        // for JPA
    }

    public WorkflowEntity(UUID id, UUID ownerId, String name, String description, Instant now) {
        this.id = id;
        this.ownerId = ownerId;
        this.name = name;
        this.description = description;
        this.latestVersion = 1;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void newDraftCreated(int version, Instant now) {
        this.latestVersion = version;
        this.updatedAt = now;
    }

    public void versionPublished(int version, Instant now) {
        this.latestPublishedVersion = version;
        this.updatedAt = now;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOwnerId() {
        return ownerId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public int getLatestVersion() {
        return latestVersion;
    }

    public Integer getLatestPublishedVersion() {
        return latestPublishedVersion;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}

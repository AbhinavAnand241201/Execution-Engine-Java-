package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** What the workflow endpoints return. Entities never leave the application layer. */
public final class WorkflowViews {

    private WorkflowViews() {
    }

    public record WorkflowSummary(UUID id, String name, String description, int latestVersion,
                                  Integer latestPublishedVersion, Instant createdAt, Instant updatedAt) {
    }

    public record WorkflowDetail(UUID id, String name, String description, int latestVersion, Integer latestPublishedVersion,
                                 Instant createdAt, Instant updatedAt, List<VersionSummary> versions) {
    }

    /** {@code lockVersion} must be sent back with the next draft update (optimistic locking). */
    public record VersionSummary(int version, String status, long lockVersion, Instant createdAt, Instant publishedAt) {
    }

    public record VersionDetail(int version, String status, long lockVersion, Instant createdAt, Instant publishedAt,
                                JsonNode definition) {
    }

    public record PublishResult(int version) {
    }
}

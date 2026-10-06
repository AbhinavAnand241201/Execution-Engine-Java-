package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.orchestrator.application.WorkflowViews.PublishResult;
import com.flowforge.orchestrator.application.WorkflowViews.VersionDetail;
import com.flowforge.orchestrator.application.WorkflowViews.VersionSummary;
import com.flowforge.orchestrator.application.WorkflowViews.WorkflowDetail;
import com.flowforge.orchestrator.application.WorkflowViews.WorkflowSummary;
import com.flowforge.orchestrator.infrastructure.persistence.TaskDefinitionWriter;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowEntity;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowSpecifications;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowVersionEntity;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowVersionRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Workflows and their versions. A version is a DRAFT (editable, one per workflow) until published; after that it
 * is immutable forever, because executions run against it. Two kinds of locking, chosen by how often they collide:
 * <ul>
 *   <li>Editing a draft: optimistic ({@code lockVersion}). Collisions are rare and the client can simply reload.
 *   Lock versions keep increasing across all versions of a workflow, so a token can only ever match one row.</li>
 *   <li>Creating the next draft and publishing: a row lock on the workflow, because they move its version counters.
 *   Publish also bumps the draft's lock version when it freezes it, so an edit and a publish that overlap can't both
 *   win: whichever versioned UPDATE runs second matches 0 rows.</li>
 * </ul>
 * Editing a draft does not touch {@code workflows.updated_at}: that would add a write to an unlocked row on every
 * keystroke-save; {@code updated_at} tracks structural changes (new draft, publish).
 * Every lookup is by id <em>and</em> owner: another user's workflow is "not found", never "forbidden".
 */
@Service
public class WorkflowService {

    private final WorkflowRepository workflows;
    private final WorkflowVersionRepository versions;
    private final TaskDefinitionWriter taskRows;
    private final DefinitionValidation validation;
    private final ObjectMapper json;
    private final Clock clock;

    public WorkflowService(WorkflowRepository workflows, WorkflowVersionRepository versions, TaskDefinitionWriter taskRows,
                           DefinitionValidation validation, ObjectMapper json, Clock clock) {
        this.workflows = workflows;
        this.versions = versions;
        this.taskRows = taskRows;
        this.validation = validation;
        this.json = json;
        this.clock = clock;
    }

    @Transactional
    public WorkflowDetail create(AuthenticatedUser owner, String name, String description, JsonNode definition) {
        DefinitionValidation.Valid valid = validation.requireValid(definition);
        if (workflows.existsByOwnerIdAndName(owner.id(), name)) {
            throw nameTaken(name);
        }
        Instant now = clock.instant();
        WorkflowEntity workflow = new WorkflowEntity(UUID.randomUUID(), owner.id(), name, description, now);
        try {
            workflows.saveAndFlush(workflow);
        } catch (DataIntegrityViolationException e) {
            if (isConstraint(e, OWNER_NAME_CONSTRAINT)) {
                throw nameTaken(name); // a concurrent create of the same name won
            }
            throw e;
        }
        versions.insertDraft(UUID.randomUUID(), workflow.getId(), 1, valid.canonicalJson().toString(), 0, now);
        return detail(workflow);
    }

    /**
     * Saves a new definition into the draft. If the latest version is published, this creates the next version as a
     * draft (the published one is never touched). {@code lockVersion} must be the one the caller last read, of the
     * draft or (when starting a new draft) of the latest published version; otherwise 409 VERSION_CONFLICT.
     */
    @Transactional
    public VersionSummary updateDraft(AuthenticatedUser owner, UUID workflowId, JsonNode definition, long lockVersion) {
        // Ownership first (someone else's workflow is a 404 whatever the body), without loading the entity: the
        // next-draft path must read the workflow row fresh under its lock.
        if (!workflows.existsByIdAndOwnerId(workflowId, owner.id())) {
            throw new NotFoundException("workflow", workflowId);
        }
        String canonical = validation.requireValid(definition).canonicalJson().toString();
        WorkflowVersionEntity draft = versions.findByWorkflowIdAndStatus(workflowId, WorkflowVersionEntity.DRAFT).orElse(null);
        if (draft == null) {
            return createNextDraft(owner, workflowId, canonical, lockVersion);
        }
        if (draft.getLockVersion() != lockVersion) {
            throw versionConflict();
        }
        draft.replaceDefinition(canonical);
        try {
            // The UPDATE carries "WHERE lock_version = <read>": a concurrent editor who saved first makes it match 0 rows.
            return summary(versions.saveAndFlush(draft));
        } catch (OptimisticLockingFailureException concurrentEdit) {
            throw versionConflict();
        }
    }

    private VersionSummary createNextDraft(AuthenticatedUser owner, UUID workflowId, String canonical, long lockVersion) {
        WorkflowEntity workflow = workflows.findForUpdate(workflowId, owner.id())
            .orElseThrow(() -> new NotFoundException("workflow", workflowId));
        if (versions.findByWorkflowIdAndStatus(workflowId, WorkflowVersionEntity.DRAFT).isPresent()) {
            throw versionConflict(); // someone else started the next draft while we waited for the lock
        }
        WorkflowVersionEntity latest = versions.findByWorkflowIdAndVersion(workflowId, workflow.getLatestVersion())
            .orElseThrow(() -> new IllegalStateException("latest version row missing for workflow " + workflowId));
        if (latest.getLockVersion() != lockVersion) {
            throw versionConflict();
        }
        Instant now = clock.instant();
        int next = workflow.getLatestVersion() + 1;
        UUID draftId = UUID.randomUUID();
        // Starts above every lock version this workflow has used, so no earlier token can ever match the new draft.
        versions.insertDraft(draftId, workflowId, next, canonical, latest.getLockVersion() + 1, now);
        workflow.newDraftCreated(next, now);
        return summary(versions.findById(draftId).orElseThrow());
    }

    /** Re-validates the draft, writes its task rows, and freezes it. */
    @Transactional
    public PublishResult publish(AuthenticatedUser owner, UUID workflowId) {
        WorkflowEntity workflow = workflows.findForUpdate(workflowId, owner.id())
            .orElseThrow(() -> new NotFoundException("workflow", workflowId));
        WorkflowVersionEntity draft = versions.findByWorkflowIdAndStatus(workflowId, WorkflowVersionEntity.DRAFT)
            .orElseThrow(() -> new ApplicationException(ErrorCode.NO_DRAFT_TO_PUBLISH, "There is no draft to publish"));
        // Validated again: executor rules or limits may have changed since the draft was saved.
        DefinitionValidation.Valid valid = validation.requireValid(read(draft.getDefinition()));
        Instant now = clock.instant();
        taskRows.insert(draft.getId(), valid.canonicalJson());
        draft.publish(now);
        try {
            // Draft edits don't take the workflow lock; if one committed after we read the draft, this versioned
            // UPDATE matches 0 rows and everything (task rows included) rolls back. The caller retries the publish.
            versions.saveAndFlush(draft);
        } catch (OptimisticLockingFailureException editedMeanwhile) {
            throw versionConflict();
        }
        workflow.versionPublished(draft.getVersion(), now);
        return new PublishResult(draft.getVersion());
    }

    @Transactional(readOnly = true)
    public Page<WorkflowSummary> list(AuthenticatedUser owner, String nameContains, Boolean published, Pageable pageable) {
        return workflows.findAll(WorkflowSpecifications.ownedBy(owner.id(), nameContains, published), pageable)
            .map(WorkflowService::summary);
    }

    @Transactional(readOnly = true)
    public WorkflowDetail get(AuthenticatedUser owner, UUID workflowId) {
        return detail(find(owner, workflowId));
    }

    @Transactional(readOnly = true)
    public VersionDetail getVersion(AuthenticatedUser owner, UUID workflowId, int version) {
        find(owner, workflowId);
        WorkflowVersionEntity entity = versions.findByWorkflowIdAndVersion(workflowId, version)
            .orElseThrow(() -> new NotFoundException("workflow version " + version, workflowId));
        return new VersionDetail(entity.getVersion(), entity.getStatus(), entity.getLockVersion(), entity.getCreatedAt(),
            entity.getPublishedAt(), read(entity.getDefinition()));
    }

    public ValidationReport validate(JsonNode definition) {
        return validation.check(definition);
    }

    /** Loader for {@link PublishedDagCache}: only published versions may be cached. */
    @Transactional(readOnly = true)
    public WorkflowDefinition publishedDefinition(UUID workflowVersionId) {
        WorkflowVersionEntity version = versions.findById(workflowVersionId)
            .filter(v -> WorkflowVersionEntity.PUBLISHED.equals(v.getStatus()))
            .orElseThrow(() -> new IllegalArgumentException("not a published version: " + workflowVersionId));
        return validation.parseStored(read(version.getDefinition()));
    }

    private WorkflowEntity find(AuthenticatedUser owner, UUID workflowId) {
        return workflows.findByIdAndOwnerId(workflowId, owner.id())
            .orElseThrow(() -> new NotFoundException("workflow", workflowId));
    }

    private WorkflowDetail detail(WorkflowEntity w) {
        return new WorkflowDetail(w.getId(), w.getName(), w.getDescription(), w.getLatestVersion(),
            w.getLatestPublishedVersion(), w.getCreatedAt(), w.getUpdatedAt(),
            versions.findByWorkflowIdOrderByVersion(w.getId()).stream().map(WorkflowService::summary).toList());
    }

    private static WorkflowSummary summary(WorkflowEntity w) {
        return new WorkflowSummary(w.getId(), w.getName(), w.getDescription(), w.getLatestVersion(),
            w.getLatestPublishedVersion(), w.getCreatedAt(), w.getUpdatedAt());
    }

    private static VersionSummary summary(WorkflowVersionEntity v) {
        return new VersionSummary(v.getVersion(), v.getStatus(), v.getLockVersion(), v.getCreatedAt(), v.getPublishedAt());
    }

    private JsonNode read(String storedJson) {
        try {
            return json.readTree(storedJson);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored definition is not JSON", e);
        }
    }

    /** Postgres' default name for {@code UNIQUE (owner_id, name)} on {@code workflows}. */
    private static final String OWNER_NAME_CONSTRAINT = "workflows_owner_id_name_key";

    /** Matched by name in the driver's message so the application layer needs no JDBC-driver types. */
    private static boolean isConstraint(DataIntegrityViolationException e, String constraint) {
        String detail = e.getMostSpecificCause().getMessage();
        return detail != null && detail.contains(constraint);
    }

    private static ApplicationException nameTaken(String name) {
        return new ApplicationException(ErrorCode.WORKFLOW_NAME_TAKEN, "You already have a workflow named '" + name + "'");
    }

    private static ApplicationException versionConflict() {
        return new ApplicationException(ErrorCode.VERSION_CONFLICT,
            "The draft was changed by someone else; reload it and apply your change again");
    }
}

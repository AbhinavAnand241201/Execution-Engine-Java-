package com.flowforge.orchestrator.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.ValidationReport;
import com.flowforge.orchestrator.application.WorkflowService;
import com.flowforge.orchestrator.application.WorkflowViews.PublishResult;
import com.flowforge.orchestrator.application.WorkflowViews.VersionDetail;
import com.flowforge.orchestrator.application.WorkflowViews.VersionSummary;
import com.flowforge.orchestrator.application.WorkflowViews.WorkflowDetail;
import com.flowforge.orchestrator.application.WorkflowViews.WorkflowSummary;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Workflow definitions and their versions. All lookups are scoped to the caller. */
@RestController
@RequestMapping("/api/v1/workflows")
public class WorkflowController {

    private static final PageRequestFactory.SortWhitelist SORTS =
        new PageRequestFactory.SortWhitelist(Map.of("createdAt", "createdAt", "name", "name"), "createdAt");

    public record CreateWorkflowRequest(@NotBlank @Pattern(regexp = "^[a-z0-9][a-z0-9-]{2,99}$") String name,
                                        @Size(max = 1000) String description, @NotNull JsonNode definition) {
    }

    public record UpdateDraftRequest(@NotNull JsonNode definition, @NotNull Long lockVersion) {
    }

    public record ValidateRequest(@NotNull JsonNode definition) {
    }

    /** List filter: DRAFT = latest version not yet published; PUBLISHED = has a published version. */
    public enum ListStatus { DRAFT, PUBLISHED }

    private final WorkflowService workflows;
    private final PageRequestFactory pages;

    public WorkflowController(WorkflowService workflows, PageRequestFactory pages) {
        this.workflows = workflows;
        this.pages = pages;
    }

    @PostMapping
    public ResponseEntity<WorkflowDetail> create(@CurrentUser AuthenticatedUser caller,
                                                 @Valid @RequestBody CreateWorkflowRequest request) {
        WorkflowDetail created = workflows.create(caller, request.name(), request.description(), request.definition());
        return ResponseEntity.created(URI.create("/api/v1/workflows/" + created.id())).body(created);
    }

    @GetMapping
    public PageResponse<WorkflowSummary> list(@CurrentUser AuthenticatedUser caller,
                                              @RequestParam(defaultValue = "0") int page,
                                              @RequestParam(defaultValue = "20") int size,
                                              @RequestParam(required = false) String sort,
                                              @RequestParam(required = false) @Size(max = 100) String name,
                                              @RequestParam(required = false) ListStatus status) {
        Boolean published = status == null ? null : status == ListStatus.PUBLISHED;
        return PageResponse.from(workflows.list(caller, name, published, pages.create(page, size, sort, SORTS)));
    }

    @GetMapping("/{id}")
    public WorkflowDetail get(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id) {
        return workflows.get(caller, id);
    }

    @GetMapping("/{id}/versions/{version}")
    public VersionDetail version(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id, @PathVariable int version) {
        return workflows.getVersion(caller, id, version);
    }

    @PutMapping("/{id}/draft")
    public VersionSummary updateDraft(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id,
                                      @Valid @RequestBody UpdateDraftRequest request) {
        return workflows.updateDraft(caller, id, request.definition(), request.lockVersion());
    }

    @PostMapping("/{id}/publish")
    public PublishResult publish(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id) {
        return workflows.publish(caller, id);
    }

    @PostMapping("/validate")
    public ValidationReport validate(@Valid @RequestBody ValidateRequest request) {
        return workflows.validate(request.definition());
    }
}

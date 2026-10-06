package com.flowforge.orchestrator.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.state.WorkflowStatus;
import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.DeadLetterService;
import com.flowforge.orchestrator.application.DeadLetterService.ReplayView;
import com.flowforge.orchestrator.application.ExecutionService;
import com.flowforge.orchestrator.application.ExecutionViews.AttemptView;
import com.flowforge.orchestrator.application.ExecutionViews.EventView;
import com.flowforge.orchestrator.application.ExecutionViews.ExecuteResult;
import com.flowforge.orchestrator.application.ExecutionViews.ExecutionDetail;
import com.flowforge.orchestrator.application.ExecutionViews.ExecutionSummary;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Starting executions and reading them back. */
@RestController
public class ExecutionController {

    private static final PageRequestFactory.SortWhitelist SORTS =
        new PageRequestFactory.SortWhitelist(Map.of("createdAt", "createdAt", "status", "status"), "createdAt");
    private static final PageRequestFactory.SortWhitelist EVENT_SORTS =
        new PageRequestFactory.SortWhitelist(Map.of("sequence", "sequence"), "sequence");

    /** Both fields optional: no version = latest published; input is visible to every task as "$input". */
    public record ExecuteRequest(@Positive Integer version, JsonNode input) {
    }

    private final ExecutionService executions;
    private final DeadLetterService deadLetters;
    private final PageRequestFactory pages;

    public ExecutionController(ExecutionService executions, DeadLetterService deadLetters, PageRequestFactory pages) {
        this.executions = executions;
        this.deadLetters = deadLetters;
        this.pages = pages;
    }

    /**
     * 202 Accepted: the execution exists and will run, but nothing has run yet. A replay of an earlier request with the
     * same Idempotency-Key returns 200 with {@code Idempotent-Replayed: true}.
     */
    @PostMapping("/api/v1/workflows/{id}/execute")
    public ResponseEntity<ExecutionDetail> execute(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id,
                                                   @Valid @RequestBody(required = false) ExecuteRequest request,
                                                   @RequestHeader(value = "Idempotency-Key", required = false)
                                                   @Size(min = 1, max = 100) String idempotencyKey,
                                                   HttpServletRequest http) {
        ExecuteRequest body = request == null ? new ExecuteRequest(null, null) : request;
        ExecuteResult result = executions.execute(caller, id, body.version(), body.input(), idempotencyKey,
            CorrelationIdFilter.currentId(http));
        URI location = URI.create("/api/v1/executions/" + result.execution().id());
        if (result.replayed()) {
            return ResponseEntity.ok().location(location).header("Idempotent-Replayed", "true").body(result.execution());
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).location(location).body(result.execution());
    }

    @GetMapping("/api/v1/executions")
    public PageResponse<ExecutionSummary> list(@CurrentUser AuthenticatedUser caller,
                                               @RequestParam(defaultValue = "0") int page,
                                               @RequestParam(defaultValue = "20") int size,
                                               @RequestParam(required = false) String sort,
                                               @RequestParam(required = false) UUID workflowId,
                                               @RequestParam(required = false) WorkflowStatus status,
                                               @RequestParam(required = false)
                                               @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant createdFrom,
                                               @RequestParam(required = false)
                                               @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant createdTo) {
        return PageResponse.from(executions.list(caller, workflowId, status, createdFrom, createdTo,
            pages.create(page, size, sort, SORTS)));
    }

    @GetMapping("/api/v1/executions/{id}")
    public ExecutionDetail get(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id) {
        return executions.get(caller, id);
    }

    @GetMapping("/api/v1/executions/{id}/events")
    public PageResponse<EventView> events(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id,
                                          @RequestParam(defaultValue = "0") int page,
                                          @RequestParam(defaultValue = "50") int size) {
        return PageResponse.from(executions.events(caller, id, pages.create(page, size, "sequence,asc", EVENT_SORTS)));
    }

    @GetMapping("/api/v1/executions/{id}/tasks/{taskKey}/attempts")
    public List<AttemptView> attempts(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id,
                                      @PathVariable String taskKey) {
        return executions.attempts(caller, id, taskKey);
    }

    /** Replays every dead (FAILED_PERMANENTLY) task of a FAILED execution; owner only. */
    @PostMapping("/api/v1/executions/{id}/retry")
    public ReplayView retry(@CurrentUser AuthenticatedUser caller, @PathVariable UUID id) {
        return deadLetters.retryExecution(caller, id);
    }
}

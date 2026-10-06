package com.flowforge.orchestrator.api;

import com.flowforge.orchestrator.application.AuthenticatedUser;
import com.flowforge.orchestrator.application.DeadLetterService;
import com.flowforge.orchestrator.application.DeadLetterService.DeadLetterView;
import com.flowforge.orchestrator.application.DeadLetterService.ReplayView;
import java.util.Map;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** The dead-letter queue for operators (OPERATOR role enforced in {@code SecurityConfig}). */
@RestController
public class DeadLetterController {

    private static final PageRequestFactory.SortWhitelist SORTS =
        new PageRequestFactory.SortWhitelist(Map.of("createdAt", "createdAt"), "createdAt");

    private final DeadLetterService deadLetters;
    private final PageRequestFactory pages;

    public DeadLetterController(DeadLetterService deadLetters, PageRequestFactory pages) {
        this.deadLetters = deadLetters;
        this.pages = pages;
    }

    /** Open entries, newest first, unless {@code includeReplayed=true}. */
    @GetMapping("/api/v1/dead-letter/tasks")
    public PageResponse<DeadLetterView> list(@RequestParam(defaultValue = "0") int page,
                                             @RequestParam(defaultValue = "20") int size,
                                             @RequestParam(required = false) String sort,
                                             @RequestParam(required = false) UUID workflowId,
                                             @RequestParam(required = false) String errorCode,
                                             @RequestParam(defaultValue = "false") boolean includeReplayed) {
        return PageResponse.from(deadLetters.list(workflowId, errorCode, includeReplayed,
            pages.create(page, size, sort, SORTS)));
    }

    @PostMapping("/api/v1/dead-letter/tasks/{entryId}/replay")
    public ReplayView replay(@CurrentUser AuthenticatedUser operator, @PathVariable UUID entryId) {
        return deadLetters.replay(operator, entryId);
    }
}

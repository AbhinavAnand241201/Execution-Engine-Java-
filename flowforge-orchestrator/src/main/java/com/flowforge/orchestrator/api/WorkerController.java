package com.flowforge.orchestrator.api;

import com.flowforge.orchestrator.application.WorkerService;
import com.flowforge.orchestrator.application.WorkerService.WorkerView;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Workers for operators (OPERATOR role enforced in {@code SecurityConfig}). */
@RestController
public class WorkerController {

    private static final PageRequestFactory.SortWhitelist SORTS =
        new PageRequestFactory.SortWhitelist(Map.of("registeredAt", "registeredAt"), "registeredAt");

    private final WorkerService workers;
    private final PageRequestFactory pages;

    public WorkerController(WorkerService workers, PageRequestFactory pages) {
        this.workers = workers;
        this.pages = pages;
    }

    @GetMapping("/api/v1/workers")
    public PageResponse<WorkerView> list(@RequestParam(defaultValue = "0") int page,
                                         @RequestParam(defaultValue = "20") int size,
                                         @RequestParam(required = false) WorkerStatus status) {
        return PageResponse.from(workers.list(status == null ? null : status.name(),
            pages.create(page, size, null, SORTS)));
    }

    @GetMapping("/api/v1/workers/{id}")
    public WorkerView get(@PathVariable String id) {
        return workers.get(id);
    }

    /** 202: the worker stops taking work on its next heartbeat and finishes what it is running. */
    @PostMapping("/api/v1/workers/{id}/drain")
    public ResponseEntity<WorkerView> drain(@PathVariable String id) {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(workers.drain(id));
    }

    /** Accepted values of the {@code status} filter (anything else → 400). */
    public enum WorkerStatus { REGISTERING, ACTIVE, DRAINING, OFFLINE }
}

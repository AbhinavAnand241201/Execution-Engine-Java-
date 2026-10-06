package com.flowforge.orchestrator.testing;

import com.flowforge.orchestrator.api.PageRequestFactory;
import com.flowforge.orchestrator.api.PageResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * Endpoints that exist only on the test classpath, so API-foundation behaviour (errors, paging, logging) can be
 * tested before any real resource exists.
 */
@RestController
@RequestMapping("/api/v1/test")
class TestOnlyController {

    private static final Logger log = LoggerFactory.getLogger(TestOnlyController.class);
    private static final PageRequestFactory.SortWhitelist SORTS =
        new PageRequestFactory.SortWhitelist(Map.of("createdAt", "createdAt", "name", "name"), "createdAt");

    private final PageRequestFactory pages;

    TestOnlyController(PageRequestFactory pages) {
        this.pages = pages;
    }

    record EchoRequest(@NotBlank String name) {
    }

    @PostMapping("/echo")
    EchoRequest echo(@Valid @RequestBody EchoRequest request) {
        return request;
    }

    @GetMapping("/boom")
    String boom() {
        throw new IllegalStateException("secret detail");
    }

    /** Throws one of Spring's own status exceptions; also only produces JSON, for the 406 test. */
    @GetMapping(value = "/status", produces = "application/json")
    String status() {
        throw new ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "too big");
    }

    @GetMapping("/log")
    String log() {
        log.info("test endpoint hit");
        return "logged";
    }

    @GetMapping("/things")
    PageResponse<String> things(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size,
                                @RequestParam(required = false) String sort) {
        Pageable pageable = pages.create(page, size, sort, SORTS);
        Page<String> empty = new PageImpl<>(List.of(), pageable, 0);
        return PageResponse.from(empty);
    }
}

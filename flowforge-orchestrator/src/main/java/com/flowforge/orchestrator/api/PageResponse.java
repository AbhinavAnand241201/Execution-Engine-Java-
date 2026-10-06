package com.flowforge.orchestrator.api;

import java.util.List;
import java.util.function.Function;
import org.springframework.data.domain.Page;

/**
 * The paging envelope of every list endpoint. A record of our own rather than Spring's {@code Page}
 * JSON, whose shape is an implementation detail that has changed between Spring versions.
 */
public record PageResponse<T>(List<T> items, int page, int size, long totalElements, int totalPages) {

    public static <T> PageResponse<T> from(Page<T> page) {
        return new PageResponse<>(page.getContent(), page.getNumber(), page.getSize(), page.getTotalElements(),
            page.getTotalPages());
    }

    public static <S, T> PageResponse<T> from(Page<S> page, Function<S, T> mapper) {
        return from(page.map(mapper));
    }
}

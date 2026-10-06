package com.flowforge.orchestrator.api;

import com.flowforge.core.error.ErrorCode;
import java.util.Locale;
import java.util.Map;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;

/**
 * Turns {@code ?page=&size=&sort=field,dir} into a {@link Pageable} with limits. Sort fields are whitelisted per
 * resource: passing a client string straight into ORDER BY would let anyone sort by (and so infer) any column,
 * including ones never shown, and would turn typos into 500s instead of 400s.
 */
@Component
public class PageRequestFactory {

    public static final int MAX_PAGE_SIZE = 100;

    /**
     * @param fields       API sort name → entity property, e.g. {@code createdAt → createdAt}
     * @param defaultField used, descending, when no sort is given
     */
    public record SortWhitelist(Map<String, String> fields, String defaultField) {

        public SortWhitelist {
            fields = Map.copyOf(fields);
            if (!fields.containsKey(defaultField)) {
                // Fails when the controller is built, not as a 500 on every unsorted request.
                throw new IllegalArgumentException("default sort field '" + defaultField + "' is not whitelisted");
            }
        }
    }

    public Pageable create(int page, int size, String sort, SortWhitelist whitelist) {
        if (page < 0) {
            throw new ApiException(ErrorCode.VALIDATION_FAILED, "page must be >= 0");
        }
        if (size < 1 || size > MAX_PAGE_SIZE) {
            throw new ApiException(ErrorCode.VALIDATION_FAILED, "size must be between 1 and " + MAX_PAGE_SIZE);
        }
        return PageRequest.of(page, size, toSort(sort, whitelist));
    }

    private static Sort toSort(String sort, SortWhitelist whitelist) {
        if (sort == null || sort.isBlank()) {
            return Sort.by(Sort.Order.desc(whitelist.fields().get(whitelist.defaultField())));
        }
        String[] parts = sort.split(",", -1);
        if (parts.length > 2) {
            throw invalidSort(whitelist);
        }
        String property = whitelist.fields().get(parts[0].trim());
        if (property == null) {
            throw invalidSort(whitelist);
        }
        String direction = parts.length == 2 ? parts[1].trim().toLowerCase(Locale.ROOT) : "asc";
        return switch (direction) {
            case "asc" -> Sort.by(Sort.Order.asc(property));
            case "desc" -> Sort.by(Sort.Order.desc(property));
            default -> throw invalidSort(whitelist);
        };
    }

    private static ApiException invalidSort(SortWhitelist whitelist) {
        return new ApiException(ErrorCode.INVALID_SORT,
            "sort must be '<field>[,asc|desc]' with field one of " + whitelist.fields().keySet().stream().sorted().toList());
    }
}

package com.flowforge.orchestrator.infrastructure.persistence;

import jakarta.persistence.criteria.Predicate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.springframework.data.jpa.domain.Specification;

/**
 * Filters for listing workflows. Built as a JPA criteria query so optional filters don't need string-concatenated
 * SQL. The name filter escapes LIKE wildcards: a user searching for "50%" means the text, not a pattern.
 */
public final class WorkflowSpecifications {

    private WorkflowSpecifications() {
    }

    /**
     * @param nameContains case-insensitive substring of the name, or null
     * @param published    null = any; true = has a published version; false = its latest version is an unpublished draft
     */
    public static Specification<WorkflowEntity> ownedBy(UUID ownerId, String nameContains, Boolean published) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();
            predicates.add(cb.equal(root.get("ownerId"), ownerId));
            if (nameContains != null && !nameContains.isBlank()) {
                String pattern = "%" + escapeLike(nameContains.toLowerCase(Locale.ROOT)) + "%";
                predicates.add(cb.like(cb.lower(root.get("name")), pattern, '\\'));
            }
            if (Boolean.TRUE.equals(published)) {
                predicates.add(cb.isNotNull(root.get("latestPublishedVersion")));
            } else if (Boolean.FALSE.equals(published)) {
                predicates.add(cb.or(cb.isNull(root.get("latestPublishedVersion")),
                    cb.lessThan(root.get("latestPublishedVersion"), root.get("latestVersion"))));
            }
            return cb.and(predicates.toArray(Predicate[]::new));
        };
    }

    private static String escapeLike(String text) {
        return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }
}

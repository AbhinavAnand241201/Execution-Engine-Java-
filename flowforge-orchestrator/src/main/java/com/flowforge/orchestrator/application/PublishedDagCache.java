package com.flowforge.orchestrator.application;

import com.flowforge.core.dag.Dag;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.util.UUID;
import java.util.function.Function;

/**
 * Keeps the {@link Dag} of each published version in memory. Caching is trivial here because published versions
 * never change (service rule + database trigger): an entry can never go stale, so there is no invalidation, only
 * a size bound for memory.
 */
public final class PublishedDagCache {

    private final LoadingCache<UUID, Dag> cache;

    /** @param loader returns the definition of a <em>published</em> version, and must fail for anything else */
    public PublishedDagCache(Function<UUID, WorkflowDefinition> loader, long maximumSize) {
        this.cache = Caffeine.newBuilder().maximumSize(maximumSize).build(versionId -> Dag.of(loader.apply(versionId)));
    }

    public Dag dag(UUID workflowVersionId) {
        return cache.get(workflowVersionId);
    }
}

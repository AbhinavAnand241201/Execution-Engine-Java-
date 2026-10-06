package com.flowforge.orchestrator.application;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.core.dag.Dag;
import com.flowforge.core.testing.WorkflowFixtures;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class PublishedDagCacheTest {

    @Test
    @DisplayName("T08.extra-4 the DAG of a published version is built once and then served from the cache")
    void loadsOncePerVersion() {
        AtomicInteger loads = new AtomicInteger();
        PublishedDagCache cache = new PublishedDagCache(versionId -> {
            loads.incrementAndGet();
            return WorkflowFixtures.diamond();
        }, 100);
        UUID v1 = UUID.randomUUID();
        UUID v2 = UUID.randomUUID();

        Dag first = cache.dag(v1);
        Dag again = cache.dag(v1);
        cache.dag(v2);

        assertThat(again).isSameAs(first);
        assertThat(first.roots()).containsExactly("a");
        assertThat(loads).hasValue(2);
    }
}

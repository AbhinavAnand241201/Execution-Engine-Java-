package com.flowforge.orchestrator.application;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;

class PublishedDefinitionIT extends OrchestratorIntegrationTestBase {

    @Autowired
    private PublishedDagCache cache;

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    @DisplayName("T08.extra-8 the DAG cache serves published versions and refuses drafts (they could still change)")
    void cacheOnlyAcceptsPublishedVersions() {
        ApiClient api = api();
        String token = api.newUserToken();
        String id = api.read(api.send(HttpMethod.POST, "/api/v1/workflows",
            Definitions.createRequest(Definitions.uniqueName("cache"), Definitions.diamond()), token)).get("id").asText();
        UUID versionId = jdbc.queryForObject("SELECT id FROM workflow_versions WHERE workflow_id = ? AND version = 1",
            UUID.class, UUID.fromString(id));

        assertThatThrownBy(() -> cache.dag(versionId)).hasMessageContaining("not a published version");

        api.send(HttpMethod.POST, "/api/v1/workflows/" + id + "/publish", null, token);
        assertThat(cache.dag(versionId).roots()).containsExactly("a");
    }
}

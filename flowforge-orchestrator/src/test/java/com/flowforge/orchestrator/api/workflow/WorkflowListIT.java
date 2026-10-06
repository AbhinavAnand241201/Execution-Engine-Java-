package com.flowforge.orchestrator.api.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;

class WorkflowListIT extends OrchestratorIntegrationTestBase {

    @Test
    @DisplayName("T08.9 list: paging, name filter, status filter, sort; users only see their own workflows")
    void listing() {
        ApiClient api = api();
        String token = api.newUserToken();
        String tag = UUID.randomUUID().toString().substring(0, 8);
        String gammaId = null;
        for (String name : new String[] {"gamma", "alpha", "beta"}) {
            String id = api.read(api.send(HttpMethod.POST, "/api/v1/workflows",
                Definitions.createRequest(name + "-" + tag, Definitions.linear(1)), token)).get("id").asText();
            if (name.equals("gamma")) {
                gammaId = id;
            }
        }
        api.send(HttpMethod.POST, "/api/v1/workflows/" + gammaId + "/publish", null, token);

        JsonNode page0 = api.read(api.get("/api/v1/workflows?name=" + tag + "&size=2&sort=name,asc", token));
        assertThat(page0.get("totalElements").asLong()).isEqualTo(3);
        assertThat(page0.get("totalPages").asInt()).isEqualTo(2);
        assertThat(page0.get("items")).extracting(i -> i.get("name").asText())
            .containsExactly("alpha-" + tag, "beta-" + tag);
        JsonNode page1 = api.read(api.get("/api/v1/workflows?name=" + tag + "&size=2&page=1&sort=name,asc", token));
        assertThat(page1.get("items")).extracting(i -> i.get("name").asText()).containsExactly("gamma-" + tag);

        JsonNode published = api.read(api.get("/api/v1/workflows?name=" + tag + "&status=PUBLISHED", token));
        assertThat(published.get("items")).extracting(i -> i.get("name").asText()).containsExactly("gamma-" + tag);
        JsonNode drafts = api.read(api.get("/api/v1/workflows?name=" + tag + "&status=DRAFT&sort=name,desc", token));
        assertThat(drafts.get("items")).extracting(i -> i.get("name").asText()).containsExactly("beta-" + tag, "alpha-" + tag);

        JsonNode newestFirst = api.read(api.get("/api/v1/workflows?name=" + tag, token));
        assertThat(newestFirst.get("items")).extracting(i -> i.get("name").asText())
            .containsExactly("beta-" + tag, "alpha-" + tag, "gamma-" + tag);
        JsonNode oldestFirst = api.read(api.get("/api/v1/workflows?name=" + tag + "&sort=createdAt,asc", token));
        assertThat(oldestFirst.get("items").get(0).get("name").asText()).isEqualTo("gamma-" + tag);

        String stranger = api.newUserToken();
        assertThat(api.read(api.get("/api/v1/workflows?name=" + tag, stranger)).get("totalElements").asLong()).isZero();

        assertThat(api.read(api.get("/api/v1/workflows?sort=owner,asc", token)).get("error").asText()).isEqualTo("INVALID_SORT");
        assertThat(api.read(api.get("/api/v1/workflows?status=ARCHIVED", token)).get("error").asText())
            .isEqualTo("VALIDATION_FAILED");
        assertThat(api.read(api.get("/api/v1/workflows?name=" + "n".repeat(101), token)).get("error").asText())
            .isEqualTo("VALIDATION_FAILED");
    }

    @Test
    @DisplayName("T08.extra-3 the name filter treats % and _ literally")
    void nameFilterEscapesWildcards() {
        ApiClient api = api();
        String token = api.newUserToken();
        api.send(HttpMethod.POST, "/api/v1/workflows", Definitions.createRequest(Definitions.uniqueName("plain"),
            Definitions.linear(1)), token);

        assertThat(api.read(api.get("/api/v1/workflows?name=%25", token)).get("totalElements").asLong()).isZero();
        assertThat(api.read(api.get("/api/v1/workflows?name=_", token)).get("totalElements").asLong()).isZero();
    }
}

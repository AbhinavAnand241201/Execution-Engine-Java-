package com.flowforge.orchestrator.api.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.testing.ConcurrencyHarness;
import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

class WorkflowVersioningIT extends OrchestratorIntegrationTestBase {

    private static String token;
    private ApiClient api;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void logIn() {
        api = api();
        token = token == null ? api.newUserToken() : token;
    }

    private String createWorkflow(Map<String, Object> definition) {
        return api.read(api.send(HttpMethod.POST, "/api/v1/workflows",
            Definitions.createRequest(Definitions.uniqueName("wf"), definition), token)).get("id").asText();
    }

    private ResponseEntity<String> putDraft(String id, Map<String, Object> definition, long lockVersion) {
        return api.send(HttpMethod.PUT, "/api/v1/workflows/" + id + "/draft",
            Map.of("definition", definition, "lockVersion", lockVersion), token);
    }

    /** The stored definition exactly as PostgreSQL returns it. */
    private String storedText(String workflowId, int version) {
        return jdbc.queryForObject("SELECT definition::text FROM workflow_versions WHERE workflow_id = ? AND version = ?",
            String.class, UUID.fromString(workflowId), version);
    }

    private ResponseEntity<String> publish(String id) {
        return api.send(HttpMethod.POST, "/api/v1/workflows/" + id + "/publish", null, token);
    }

    @Test
    @DisplayName("T08.3 updating the draft with the current lockVersion → 200 and lockVersion + 1; a stale one → 409")
    void optimisticDraftUpdate() {
        String id = createWorkflow(Definitions.linear(2));

        ResponseEntity<String> updated = putDraft(id, Definitions.linear(3), 0);
        assertThat(updated.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode version = api.read(updated);
        assertThat(version.get("version").asInt()).isEqualTo(1);
        assertThat(version.get("lockVersion").asLong()).isEqualTo(1);

        ResponseEntity<String> stale = putDraft(id, Definitions.linear(4), 0);
        assertThat(stale.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api.read(stale).get("error").asText()).isEqualTo("VERSION_CONFLICT");

        JsonNode v1 = api.read(api.get("/api/v1/workflows/" + id + "/versions/1", token));
        assertThat(v1.get("definition").get("tasks")).hasSize(3);
    }

    @Test
    @DisplayName("T08.4 two concurrent draft updates with the same lockVersion (start gate, 50 iterations) → exactly one 200, "
        + "one 409")
    void concurrentDraftUpdates() throws Exception {
        String[] workflowId = new String[1];

        List<List<Object>> rounds = ConcurrencyHarness.race(2, 50,
            thread -> putDraft(workflowId[0], Definitions.linear(2 + thread), 0).getStatusCode().value(),
            () -> workflowId[0] = createWorkflow(Definitions.linear(1)));

        for (int round = 0; round < rounds.size(); round++) {
            List<Object> statuses = rounds.get(round);
            assertThat(statuses).containsExactlyInAnyOrder(200, 409);
        }
    }

    @Test
    @DisplayName("T08.4 the winner's definition is what got stored, with lockVersion 1 (one write, not two)")
    void concurrentDraftUpdateStoresTheWinner() throws Exception {
        for (int round = 0; round < 20; round++) {
            String id = createWorkflow(Definitions.linear(1));
            List<List<Object>> outcome = ConcurrencyHarness.race(2, 1,
                thread -> putDraft(id, Definitions.linear(2 + thread), 0).getStatusCode().value());
            int winner = outcome.get(0).indexOf(200);

            JsonNode v1 = api.read(api.get("/api/v1/workflows/" + id + "/versions/1", token));
            assertThat(v1.get("lockVersion").asLong()).isEqualTo(1);
            assertThat(v1.get("definition").get("tasks")).hasSize(2 + winner);
        }
    }

    @Test
    @DisplayName("T08.extra-6 lockVersion never repeats across versions: a token for published v1 can't overwrite another "
        + "user's edit of v2")
    void staleTokenFromPublishedVersionIsRejected() {
        String id = createWorkflow(Definitions.linear(1));
        publish(id);
        long publishedLock = api.read(api.get("/api/v1/workflows/" + id + "/versions/1", token)).get("lockVersion").asLong();

        // B starts v2 from v1 and edits it once.
        JsonNode v2 = api.read(putDraft(id, Definitions.linear(2), publishedLock));
        JsonNode v2Edited = api.read(putDraft(id, Definitions.linear(3), v2.get("lockVersion").asLong()));
        assertThat(v2Edited.get("lockVersion").asLong()).isGreaterThan(publishedLock);

        // A, still holding v1's token, tries to "start a new draft from v1".
        ResponseEntity<String> stale = putDraft(id, Definitions.diamond(), publishedLock);
        assertThat(stale.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api.read(api.get("/api/v1/workflows/" + id + "/versions/2", token)).get("definition").get("tasks"))
            .hasSize(3);
    }

    @Test
    @DisplayName("T08.extra-7 concurrent creates with the same name (4 threads × 50, start gate) → one 201, three "
        + "WORKFLOW_NAME_TAKEN, never a 500")
    void concurrentCreatesWithOneName() throws Exception {
        String[] name = new String[1];

        List<List<Object>> rounds = ConcurrencyHarness.race(4, 50,
            thread -> api.send(HttpMethod.POST, "/api/v1/workflows", Definitions.createRequest(name[0],
                Definitions.linear(1)), token).getStatusCode().value(),
            () -> name[0] = Definitions.uniqueName("same"));

        for (List<Object> statuses : rounds) {
            assertThat(statuses).containsExactlyInAnyOrder(201, 409, 409, 409);
        }
    }

    @Test
    @DisplayName("T08.extra-5 publish racing a draft edit (start gate, 50 iterations): never a 500, and every published "
        + "version's task rows match its stored definition")
    void publishRacingEdit() throws Exception {
        String[] workflowId = new String[1];

        List<List<Object>> rounds = ConcurrencyHarness.race(2, 50,
            thread -> thread == 0
                ? publish(workflowId[0]).getStatusCode().value()
                : putDraft(workflowId[0], Definitions.linear(3), 0).getStatusCode().value(),
            () -> workflowId[0] = createWorkflow(Definitions.linear(2)));

        for (List<Object> statuses : rounds) {
            assertThat(statuses).doesNotContain(500);
            assertThat((Integer) statuses.get(0)).as("publish").isIn(200, 409);
            assertThat((Integer) statuses.get(1)).as("edit").isIn(200, 409);
        }
        Integer mismatches = jdbc.queryForObject("SELECT count(*) FROM workflow_versions v WHERE v.status = 'PUBLISHED' "
            + "AND jsonb_array_length(v.definition->'tasks') <> "
            + "(SELECT count(*) FROM task_definitions t WHERE t.workflow_version_id = v.id)", Integer.class);
        assertThat(mismatches).isZero();
    }

    @Test
    @DisplayName("T08.5 publish → PUBLISHED; task_definitions and task_dependencies rows match the definition")
    void publishMaterializesRows() {
        String id = createWorkflow(Definitions.diamond());

        ResponseEntity<String> published = publish(id);
        assertThat(published.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(api.read(published).get("version").asInt()).isEqualTo(1);

        JsonNode workflow = api.read(api.get("/api/v1/workflows/" + id, token));
        assertThat(workflow.get("latestPublishedVersion").asInt()).isEqualTo(1);
        JsonNode v1 = workflow.get("versions").get(0);
        assertThat(v1.get("status").asText()).isEqualTo("PUBLISHED");
        assertThat(v1.get("publishedAt").asText()).isNotBlank();

        UUID versionId = jdbc.queryForObject(
            "SELECT id FROM workflow_versions WHERE workflow_id = ? AND version = 1", UUID.class, UUID.fromString(id));
        List<Map<String, Object>> tasks = jdbc.queryForList(
            "SELECT task_key, type, timeout_ms, priority, config->>'durationMs' AS duration, retry_policy->>'maxAttempts' "
                + "AS max_attempts FROM task_definitions WHERE workflow_version_id = ? ORDER BY task_key", versionId);
        assertThat(tasks).extracting(t -> t.get("task_key")).containsExactly("a", "b", "c", "d", "e");
        assertThat(tasks).allSatisfy(t -> {
            assertThat(t.get("type")).isEqualTo("DELAY");
            assertThat(t.get("timeout_ms")).isEqualTo(60_000L);
            assertThat(t.get("priority")).isEqualTo("NORMAL");
            assertThat(t.get("duration")).isEqualTo("0");
            assertThat(t.get("max_attempts")).isEqualTo("3");
        });
        List<String> edges = jdbc.queryForList("SELECT depends_on_task_key || '->' || task_key FROM task_dependencies "
            + "WHERE workflow_version_id = ? ORDER BY 1", String.class, versionId);
        assertThat(edges).containsExactly("a->b", "a->c", "a->d", "b->e", "c->e", "d->e");
    }

    @Test
    @DisplayName("T08.6 publishing with no draft → 409 NO_DRAFT_TO_PUBLISH")
    void publishWithoutDraft() {
        String id = createWorkflow(Definitions.linear(1));
        assertThat(publish(id).getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<String> again = publish(id);
        assertThat(again.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(api.read(again).get("error").asText()).isEqualTo("NO_DRAFT_TO_PUBLISH");
    }

    @Test
    @DisplayName("T08.7 editing after publish creates v2 DRAFT; v1 is unchanged")
    void editAfterPublishCreatesNewDraft() {
        String id = createWorkflow(Definitions.linear(2));
        publish(id);
        JsonNode v1Before = api.read(api.get("/api/v1/workflows/" + id + "/versions/1", token));
        String v1TextBefore = storedText(id, 1);

        ResponseEntity<String> edited = putDraft(id, Definitions.diamond(), v1Before.get("lockVersion").asLong());
        assertThat(edited.getStatusCode()).isEqualTo(HttpStatus.OK);
        JsonNode v2 = api.read(edited);
        assertThat(v2.get("version").asInt()).isEqualTo(2);
        assertThat(v2.get("status").asText()).isEqualTo("DRAFT");

        JsonNode v1After = api.read(api.get("/api/v1/workflows/" + id + "/versions/1", token));
        assertThat(v1After).isEqualTo(v1Before);
        assertThat(storedText(id, 1)).isEqualTo(v1TextBefore);
        JsonNode workflow = api.read(api.get("/api/v1/workflows/" + id, token));
        assertThat(workflow.get("latestVersion").asInt()).isEqualTo(2);
        assertThat(workflow.get("latestPublishedVersion").asInt()).isEqualTo(1);

        // A second edit updates the same v2 draft rather than creating v3.
        assertThat(api.read(putDraft(id, Definitions.linear(3), v2.get("lockVersion").asLong())).get("version").asInt())
            .isEqualTo(2);
    }

    @Test
    @DisplayName("T08.8 raw SQL UPDATE or DELETE of a published version is rejected by the database trigger")
    void triggerProtectsPublishedVersions() {
        String id = createWorkflow(Definitions.linear(1));
        publish(id);
        UUID workflowId = UUID.fromString(id);

        assertThatThrownBy(() -> jdbc.update(
            "UPDATE workflow_versions SET definition = '{\"tasks\": []}' WHERE workflow_id = ? AND version = 1", workflowId))
            .isInstanceOf(DataAccessException.class).hasMessageContaining("immutable");
        assertThatThrownBy(() -> jdbc.update("DELETE FROM workflow_versions WHERE workflow_id = ? AND version = 1", workflowId))
            .isInstanceOf(DataAccessException.class).hasMessageContaining("immutable");
        assertThatThrownBy(() -> jdbc.update(
            "UPDATE workflow_versions SET status = 'DRAFT' WHERE workflow_id = ? AND version = 1", workflowId))
            .isInstanceOf(DataAccessException.class).hasMessageContaining("immutable");

        // Drafts are not protected: they're meant to change.
        String draftId = createWorkflow(Definitions.linear(1));
        assertThat(jdbc.update("UPDATE workflow_versions SET created_at = created_at WHERE workflow_id = ?",
            UUID.fromString(draftId))).isEqualTo(1);
    }
}

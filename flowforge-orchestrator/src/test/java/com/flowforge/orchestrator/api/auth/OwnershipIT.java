package com.flowforge.orchestrator.api.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.flowforge.orchestrator.testing.ApiClient;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class OwnershipIT extends OrchestratorIntegrationTestBase {

    @Test
    @DisplayName("T07.5 user B reading user A's resource → 404, with the same body as a resource that doesn't exist")
    void otherUsersResourceIsNotFound() {
        ApiClient api = api();
        String alice = api.newUserToken();
        String bob = api.newUserToken();
        String noteId = api.read(api.send(HttpMethod.POST, "/api/v1/test/notes", null, alice)).get("id").asText();

        assertThat(api.get("/api/v1/test/notes/" + noteId, alice).getStatusCode()).isEqualTo(HttpStatus.OK);

        ResponseEntity<String> notYours = api.get("/api/v1/test/notes/" + noteId, bob);
        ResponseEntity<String> missing = api.get("/api/v1/test/notes/00000000-0000-4000-8000-000000000000", bob);
        assertThat(notYours.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(api.read(notYours).get("error")).isEqualTo(api.read(missing).get("error"));
        assertThat(api.read(notYours).get("message").asText()).doesNotContain(noteId);
    }
}

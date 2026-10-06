package com.flowforge.core.state;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StateMachinesDocumentTest {

    /** Found by walking up to the repository root, so the test works from Surefire (module dir) and IDEs (repo root). */
    private static final Path DOCUMENT = StateMachinesDocument.defaultLocation();

    @Test
    @DisplayName("T03.5 docs/state-machines.md equals the freshly rendered diagrams (regenerate with StateMachinesDocument.main)")
    void documentMatchesCode() throws Exception {
        assertThat(DOCUMENT).exists();
        // CRLF normalised in case a checkout ignored .gitattributes.
        assertThat(Files.readString(DOCUMENT).replace("\r\n", "\n"))
            .as("%s is stale; regenerate with: "
                + "java -cp flowforge-core/target/classes com.flowforge.core.state.StateMachinesDocument", DOCUMENT)
            .isEqualTo(StateMachinesDocument.render());
    }

    @Test
    @DisplayName("T03.5 the rendered diagram contains every allowed edge and marks terminal states")
    void rendererIncludesEdgesAndTerminals() {
        String diagram = StateDiagramRenderer.render(TaskStateMachine.INSTANCE, TaskStatus.PENDING);

        assertThat(diagram).startsWith("stateDiagram-v2");
        assertThat(diagram).contains("[*] --> PENDING", "RUNNING --> RETRYING", "FAILED_PERMANENTLY --> READY",
            "SUCCEEDED --> [*]", "CANCELLED --> [*]");
        long edges = diagram.lines().filter(line -> line.contains(" --> ") && !line.contains("[*]")).count();
        assertThat(edges).isEqualTo(13);
    }
}

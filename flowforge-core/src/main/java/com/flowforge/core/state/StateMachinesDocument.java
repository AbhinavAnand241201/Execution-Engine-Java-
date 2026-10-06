package com.flowforge.core.state;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Generates {@code docs/state-machines.md} from the state machine tables. A snapshot test compares the committed
 * file with {@link #render()}, so a table change without regenerating the doc fails the build.
 *
 * <p>Regenerate from the repository root (the output file is located from any directory; the {@code -cp} path is not):
 * {@code java -cp flowforge-core/target/classes com.flowforge.core.state.StateMachinesDocument}
 */
public final class StateMachinesDocument {

    private StateMachinesDocument() {
    }

    public static String render() {
        return """
            # State machines

            > Generated from `TaskStateMachine` and `WorkflowStateMachine` by `StateMachinesDocument`. Do not edit by hand:
            > the build fails (T03.5) if this file and the code disagree.

            ## Task execution

            FAILED is not a task state: a failed attempt is recorded as an attempt outcome and the task moves straight to
            RETRYING or FAILED_PERMANENTLY in the same transaction.

            ```mermaid
            %s```

            ## Workflow execution

            ```mermaid
            %s```

            ## Attempt outcomes

            `SUCCEEDED`, `FAILED`, `TIMED_OUT`, `WORKER_LOST`, `WORKER_SHUTDOWN`, `CANCELLED`. `TIMED_OUT`, `WORKER_LOST`
            and `WORKER_SHUTDOWN` are always retryable; `FAILED` is retryable if the executor says so.
            """.formatted(
                StateDiagramRenderer.render(TaskStateMachine.INSTANCE, TaskStatus.PENDING),
                StateDiagramRenderer.render(WorkflowStateMachine.INSTANCE, WorkflowStatus.CREATED));
    }

    /** The committed document, found from any working directory inside the repository. */
    public static Path defaultLocation() {
        return repositoryRoot(Path.of("").toAbsolutePath()).resolve("docs").resolve("state-machines.md");
    }

    /** Walks up from {@code start} to the repository root: the directory that holds the {@code flowforge-core} module. */
    static Path repositoryRoot(Path start) {
        for (Path dir = start; dir != null; dir = dir.getParent()) {
            if (Files.exists(dir.resolve("pom.xml")) && Files.exists(dir.resolve("flowforge-core").resolve("pom.xml"))) {
                return dir;
            }
        }
        throw new IllegalStateException("not inside the FlowForge repository (no root pom.xml above " + start + ")");
    }

    public static void main(String[] args) throws IOException {
        Path target = args.length > 0 ? Path.of(args[0]) : defaultLocation();
        Files.writeString(target, render());
        System.out.println("wrote " + target.toAbsolutePath());
    }
}

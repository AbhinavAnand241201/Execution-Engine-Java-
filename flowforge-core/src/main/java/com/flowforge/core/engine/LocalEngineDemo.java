package com.flowforge.core.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.executor.DelayTaskExecutor;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.WorkflowDefinition;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

/**
 * Demo: runs the diamond {@code a → (b, c, d) → e} with DELAY tasks and prints the timeline, showing b, c
 * and d running at the same time. From the repository root:
 * <pre>
 * ./mvnw -q -pl flowforge-core compile dependency:build-classpath -Dmdep.outputFile=target/cp.txt
 * java -cp flowforge-core/target/classes:$(cat flowforge-core/target/cp.txt) com.flowforge.core.engine.LocalEngineDemo
 * </pre>
 */
public final class LocalEngineDemo {

    private LocalEngineDemo() {
    }

    public static void main(String[] args) throws Exception {
        WorkflowDefinition diamond = WorkflowDefinition.builder()
            .task("a", TaskType.DELAY).config(delay(100))
            .task("b", TaskType.DELAY).config(delay(300)).dependsOn("a")
            .task("c", TaskType.DELAY).config(delay(300)).dependsOn("a")
            .task("d", TaskType.DELAY).config(delay(300)).dependsOn("a")
            .task("e", TaskType.DELAY).config(delay(100)).dependsOn("b", "c", "d")
            .build();

        try (LocalWorkflowEngine engine = new LocalWorkflowEngine(new TaskExecutorRegistry(List.of(new DelayTaskExecutor())),
            new LocalEngineOptions(ExecutionMode.PLATFORM, 4), Clock.systemUTC(), new SplittableRandom())) {
            ExecutionReport report = engine.start(diamond, null).completion().get(10, TimeUnit.SECONDS);

            Instant start = report.timeline().get(0).at();
            System.out.println("t(ms)  task  event");
            for (TimelineEvent event : report.timeline()) {
                if (event.type() == TimelineEvent.Type.QUEUED) {
                    continue;
                }
                long millis = Duration.between(start, event.at()).toMillis();
                String task = event.taskKey() == null ? "-" : event.taskKey();
                String what = event.type() == TimelineEvent.Type.WORKFLOW_STATUS ? "workflow " + event.detail() : event.type().name();
                System.out.printf("%5d  %-4s  %s%n", millis, task, what);
            }
            System.out.println("final status: " + report.status());
        }
    }

    private static JsonNode delay(long millis) {
        return JsonNodeFactory.instance.objectNode().put("durationMs", millis);
    }
}

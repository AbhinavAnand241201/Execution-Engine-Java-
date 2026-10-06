package com.flowforge.runtime.claim;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.testing.EngineDatabase;
import com.flowforge.runtime.testing.EngineDatabase.Execution;
import com.flowforge.runtime.testing.IntegrationTestBase;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TaskInputRepositoryIT extends IntegrationTestBase {

    @Test
    @DisplayName("T11.extra-1 inputs = execution input as $input + outputs of SUCCEEDED dependsOn tasks only")
    void loadsInputs() {
        Execution execution = EngineDatabase.insertExecution("RUNNING", "{\"orderId\":7}", "PENDING", 0, 1_000,
            "a", "b", "c", "d");
        EngineDatabase.setTask(execution.tasks()[0], "SUCCEEDED", "{\"rows\":[1,2]}");
        EngineDatabase.setTask(execution.tasks()[1], "SUCCEEDED", "{\"ok\":true}");
        EngineDatabase.setTask(execution.tasks()[2], "RUNNING", null);
        TaskCommand command = new TaskCommand(UUID.randomUUID(), 1, execution.tasks()[3], execution.id(), "d",
            TaskType.DELAY, 1, null, List.of("a", "c"), 1_000, "corr", Instant.now());

        var inputs = new TaskInputRepository(EngineDatabase.dataSource(), new ObjectMapper()).load(command);

        assertThat(inputs).containsOnlyKeys("$input", "a");
        assertThat(inputs.get("$input").get("orderId").asInt()).isEqualTo(7);
        assertThat(inputs.get("a").get("rows")).hasSize(2);
    }
}

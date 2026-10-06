package com.flowforge.core.dag;

import com.flowforge.core.workflow.TaskDefinition;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.ValidationErrorCode;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.core.workflow.WorkflowDefinitionValidator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The full validation of a definition: structural rules plus "the graph has no cycle". Composition instead of
 * adding cycles to {@link WorkflowDefinitionValidator} keeps the {@code workflow} package free of graph code (the
 * {@code dag} package depends on {@code workflow}, never the reverse).
 *
 * <p>Collect-all still holds: the cycle check runs on whatever graph can be built even when structural errors
 * exist. The error points at the dependency entry that closes the cycle. Self edges are left out of that graph
 * because they're already reported as {@code SELF_DEPENDENCY}.
 */
public final class DagValidator {

    private final WorkflowDefinitionValidator structural;
    private final CycleDetector cycleDetector = new CycleDetector();

    public DagValidator(WorkflowDefinitionValidator structural) {
        this.structural = structural;
    }

    public List<ValidationError> validate(WorkflowDefinition definition) {
        List<ValidationError> errors = new ArrayList<>(structural.validate(definition));
        Optional<List<String>> cycle = cycleDetector.findCycle(Dag.lenient(definition, false));
        cycle.ifPresent(path -> errors.add(new ValidationError(
            closingEdgePath(definition, path),
            ValidationErrorCode.CYCLE_DETECTED,
            "cycle detected: " + CycleDetector.render(path))));
        return List.copyOf(errors);
    }

    /**
     * Points at the dependency entry that closes the cycle: in {@code a → b → c → a}, task {@code a}'s entry for
     * {@code c}. Same path style as the structural errors ({@code tasks[i].dependsOn[j]}).
     */
    private static String closingEdgePath(WorkflowDefinition definition, List<String> cycle) {
        String task = cycle.get(0);
        String closingDependency = cycle.get(cycle.size() - 2);
        List<TaskDefinition> tasks = definition.tasks();
        for (int i = 0; i < tasks.size(); i++) {
            if (task.equals(tasks.get(i).key())) {
                return "tasks[" + i + "].dependsOn[" + tasks.get(i).dependsOn().indexOf(closingDependency) + "]";
            }
        }
        throw new IllegalStateException("cycle node not in definition: " + task);
    }
}

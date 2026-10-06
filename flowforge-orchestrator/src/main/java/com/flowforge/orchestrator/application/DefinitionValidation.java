package com.flowforge.orchestrator.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.dag.Dag;
import com.flowforge.core.dag.DagValidator;
import com.flowforge.core.dag.TopologicalSorter;
import com.flowforge.core.workflow.InvalidWorkflowDefinitionException;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.WorkflowDefinition;
import com.flowforge.core.workflow.WorkflowDefinitionParser;
import java.util.List;

/**
 * The one place a submitted definition is checked: strict parsing, then every structural rule, the cycle check and
 * each executor's config rules. Create, draft update and publish all call it, so a definition can't get in (or get
 * published) through a path that skips a rule.
 */
public class DefinitionValidation {

    private final WorkflowDefinitionParser parser;
    private final DagValidator validator;
    private final TopologicalSorter sorter = new TopologicalSorter();

    public DefinitionValidation(WorkflowDefinitionParser parser, DagValidator validator) {
        this.parser = parser;
        this.validator = validator;
    }

    /** A definition that passed every check, with its canonical JSON (all defaults written out). */
    public record Valid(WorkflowDefinition definition, JsonNode canonicalJson) {
    }

    /** Throws {@link InvalidWorkflowDefinitionException} (422, every problem listed) unless the definition is valid. */
    public Valid requireValid(JsonNode json) {
        WorkflowDefinition definition = parser.parse(json);
        List<ValidationError> errors = validator.validate(definition);
        if (!errors.isEmpty()) {
            throw new InvalidWorkflowDefinitionException(errors);
        }
        return new Valid(definition, parser.toJsonTree(definition));
    }

    /** Dry run: problems as data, plus the parallel levels when there are none. */
    public ValidationReport check(JsonNode json) {
        try {
            Valid valid = requireValid(json);
            return new ValidationReport(true, List.of(), sorter.levels(Dag.of(valid.definition())));
        } catch (InvalidWorkflowDefinitionException e) {
            return new ValidationReport(false, e.errors(), List.of());
        }
    }

    /** Parses a stored (already validated) definition. */
    public WorkflowDefinition parseStored(JsonNode json) {
        return parser.parse(json);
    }
}

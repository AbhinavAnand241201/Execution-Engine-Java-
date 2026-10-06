package com.flowforge.orchestrator.application;

import com.flowforge.core.workflow.ValidationError;
import java.util.List;

/** Result of {@code POST /workflows/validate}: {@code levels} are the groups of tasks that could run in parallel. */
public record ValidationReport(boolean valid, List<ValidationError> errors, List<List<String>> levels) {
}

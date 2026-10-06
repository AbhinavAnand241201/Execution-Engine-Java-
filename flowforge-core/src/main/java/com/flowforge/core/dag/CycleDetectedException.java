package com.flowforge.core.dag;

import com.flowforge.core.error.ErrorCode;
import com.flowforge.core.error.FlowForgeException;
import java.util.List;

/**
 * Thrown when a graph that should be acyclic isn't. Validation normally reports cycles as data; this exception is
 * for code paths that receive a graph and can't continue, such as sorting.
 */
public class CycleDetectedException extends FlowForgeException {

    private final List<String> cycle;

    public CycleDetectedException(List<String> cycle) {
        super(ErrorCode.INVALID_WORKFLOW_DEFINITION, "cycle detected: " + CycleDetector.render(cycle));
        this.cycle = List.copyOf(cycle);
    }

    public List<String> cycle() {
        return cycle;
    }
}

package com.flowforge.orchestrator.engine;

import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.runtime.runner.TaskResultSink;

/** Local mode: a finished attempt goes straight to the result processor, in its own transaction. */
public class LocalResultSink implements TaskResultSink {

    private final ResultProcessor results;

    public LocalResultSink(ResultProcessor results) {
        this.results = results;
    }

    @Override
    public void publish(TaskResultMessage result) {
        results.process(result);
    }
}

package com.flowforge.runtime.runner;

import com.flowforge.core.contracts.TaskResultMessage;

/**
 * Where finished attempts go (port). Local mode hands results straight to the orchestrator's result processor;
 * the worker publishes them to Kafka. Throwing means "not delivered": the runner retries with backoff.
 */
@FunctionalInterface
public interface TaskResultSink {

    void publish(TaskResultMessage result) throws Exception;
}

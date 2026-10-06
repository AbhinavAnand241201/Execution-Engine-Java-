package com.flowforge.orchestrator.engine;

import com.flowforge.core.contracts.TaskCommand;

/**
 * Sends a command to whoever runs tasks (port). Called inside the dispatch transaction; implementations must only
 * deliver after that transaction commits: local mode registers an afterCommit hook, Kafka mode writes the
 * outbox in the same transaction.
 */
@FunctionalInterface
public interface TaskDispatcher {

    void dispatch(TaskCommand command);
}

package com.flowforge.orchestrator.engine;

import com.flowforge.core.workflow.TaskType;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;

/**
 * Local mode: one runner runs every type, so capacity is shared: max-concurrency minus everything QUEUED or RUNNING
 *. Counted from the database. For a moment the count can be one lower than what the runner holds (a
 * task leaves RUNNING when its result commits, just before its permit is released); the runner's own semaphore covers
 * that gap by answering RETRY_LATER, and the command is delivered again 200 ms later.
 */
public class LocalCapacityProvider implements CapacityProvider {

    private final EngineJdbcRepository engine;
    private final int maxConcurrency;

    public LocalCapacityProvider(EngineJdbcRepository engine, int maxConcurrency) {
        this.engine = engine;
        this.maxConcurrency = maxConcurrency;
    }

    @Override
    public Budget budget() {
        int inFlight = engine.inFlightByType().values().stream().mapToInt(Integer::intValue).sum();
        int[] remaining = {maxConcurrency - inFlight};
        return new Budget() {
            @Override
            public boolean tryTake(TaskType type) {
                if (remaining[0] <= 0) {
                    return false;
                }
                remaining[0]--;
                return true;
            }

            @Override
            public void giveBack(TaskType type) {
                remaining[0]++;
            }
        };
    }
}

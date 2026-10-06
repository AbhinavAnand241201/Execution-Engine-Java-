package com.flowforge.orchestrator.kafka;

import com.flowforge.core.workflow.TaskType;
import com.flowforge.orchestrator.engine.CapacityProvider;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository;
import java.util.EnumMap;
import java.util.Map;

/**
 * Kafka mode capacity per task type: Σ max_concurrency of the ACTIVE workers that have the capability,
 * minus what of that type is QUEUED or RUNNING. DRAINING and OFFLINE workers count for nothing, and a type no live
 * worker can run has capacity 0, so its tasks stay READY (visible in {@code queue.depth}) instead of piling up in a
 * topic nobody reads. Priorities keep working: the highest-priority READY tasks take the free slots.
 */
public class KafkaCapacityProvider implements CapacityProvider {

    private final EngineJdbcRepository engine;
    private final WorkerJdbcRepository workers;

    public KafkaCapacityProvider(EngineJdbcRepository engine, WorkerJdbcRepository workers) {
        this.engine = engine;
        this.workers = workers;
    }

    @Override
    public Budget budget() {
        Map<TaskType, Integer> inFlight = engine.inFlightByType();
        Map<TaskType, Integer> capacity = workers.activeCapacityByType();
        Map<TaskType, Integer> remaining = new EnumMap<>(TaskType.class);
        for (TaskType type : TaskType.values()) {
            remaining.put(type, capacity.getOrDefault(type, 0) - inFlight.getOrDefault(type, 0));
        }
        return new Budget() {
            @Override
            public boolean tryTake(TaskType type) {
                if (remaining.get(type) <= 0) {
                    return false;
                }
                remaining.merge(type, -1, Integer::sum);
                return true;
            }

            @Override
            public void giveBack(TaskType type) {
                remaining.merge(type, 1, Integer::sum);
            }
        };
    }
}

package com.flowforge.worker.messaging;

import com.flowforge.core.contracts.Topics;
import com.flowforge.core.workflow.TaskType;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/**
 * One listener container per capability: it subscribes to {@code ff.task.commands.<type>} in consumer group
 * {@code ff-worker-<type>}, so each command goes to one worker of that type, and a worker without a capability never
 * sees its commands. Started, drained and stopped by the worker's lifecycle.
 */
public class CommandConsumers {

    private static final Logger log = LoggerFactory.getLogger(CommandConsumers.class);

    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;
    private final CommandListener listener;
    private final Set<TaskType> capabilities;
    private final int concurrency;
    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();
    private boolean drainRequested;
    private volatile boolean drained;

    @SuppressWarnings("unchecked")   // Boot declares its factory as <?, ?>; this app only uses String keys and values
    public CommandConsumers(ConcurrentKafkaListenerContainerFactory<?, ?> factory, CommandListener listener,
                            Set<TaskType> capabilities, int concurrency) {
        this.factory = (ConcurrentKafkaListenerContainerFactory<String, String>) factory;
        this.listener = listener;
        this.capabilities = Set.copyOf(capabilities);
        this.concurrency = concurrency;
    }

    public synchronized void start() {
        for (TaskType type : capabilities) {
            ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(
                Topics.taskCommands(type));
            container.getContainerProperties().setGroupId("ff-worker-" + type.name().toLowerCase(Locale.ROOT));
            container.getContainerProperties().setMessageListener(listener);
            container.setConcurrency(concurrency);
            container.setBeanName("commands-" + type.name().toLowerCase(Locale.ROOT));
            // On stop, don't run the rest of the polled batch: a draining worker must not claim anything new.
            container.getContainerProperties().setStopImmediate(true);
            container.start();
            containers.add(container);
        }
        log.info("listening for task commands capabilities={}", capabilities);
    }

    /**
     * Drain: stop taking commands for good. The containers stop, so this worker leaves its consumer groups and its
     * partitions are rebalanced to the other workers of each type: a paused consumer would keep owning them and
     * their commands would wait for nobody. In-flight attempts are not touched; they finish and report.
     */
    public void drain() {
        synchronized (this) {
            if (drainRequested) {
                return;
            }
            drainRequested = true;
        }
        stop();
        drained = true;
        log.info("draining: left the command consumer groups");
    }

    /** True once the containers have actually stopped (not just when the drain was requested). */
    public boolean isDraining() {
        return drained;
    }

    /** Leaves the consumer groups; uncommitted commands go to other workers of the type. */
    public synchronized void stop() {
        containers.forEach(ConcurrentMessageListenerContainer::stop);
        containers.clear();
    }
}

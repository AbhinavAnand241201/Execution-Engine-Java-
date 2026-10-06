package com.flowforge.orchestrator.kafka;

import com.flowforge.core.contracts.Topics;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

/** The listener container of {@link ResultConsumer}: group {@code ff-orchestrator} on {@code ff.task.results}. */
public class ResultConsumers implements SmartLifecycle {

    private final ConcurrentMessageListenerContainer<String, String> container;

    @SuppressWarnings("unchecked")   // Boot declares its factory as <?, ?>; this app only uses String keys and values
    public ResultConsumers(ConcurrentKafkaListenerContainerFactory<?, ?> factory, ResultConsumer consumer,
                           int concurrency) {
        container = ((ConcurrentKafkaListenerContainerFactory<String, String>) factory).createContainer(
            Topics.TASK_RESULTS);
        container.getContainerProperties().setGroupId("ff-orchestrator");
        container.getContainerProperties().setMessageListener(consumer);
        container.setConcurrency(concurrency);
        container.setBeanName("results");
    }

    @Override
    public void start() {
        container.start();
    }

    @Override
    public void stop() {
        container.stop();
    }

    @Override
    public boolean isRunning() {
        return container.isRunning();
    }
}

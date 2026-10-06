package com.flowforge.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.kafka.topics.*}: partitions and replication of the topics the orchestrator creates. */
@ConfigurationProperties("flowforge.kafka.topics")
public record KafkaTopicsProperties(int partitions, short replicationFactor) {
}

package com.flowforge.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.Topics;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.orchestrator.engine.RecurringJob;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxJanitor;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxRelay;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import com.flowforge.orchestrator.kafka.InvalidMessageException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import com.flowforge.runtime.db.DatabaseErrors;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Kafka topics (created by the orchestrator at startup: auto-creation is off on the broker) and the
 * transactional outbox jobs.
 */
@Configuration
public class MessagingConfig {

    /** Every topic, plus the dead-letter topics of the two consumed streams (commands, results). */
    @Bean
    KafkaAdmin.NewTopics flowforgeTopics(KafkaTopicsProperties topics) {
        List<NewTopic> all = new ArrayList<>();
        for (TaskType type : TaskType.values()) {
            all.add(topic(Topics.taskCommands(type), topics.partitions(), topics));
            all.add(topic(Topics.taskCommands(type) + Topics.DEAD_LETTER_SUFFIX, topics.partitions(), topics));
        }
        all.add(topic(Topics.TASK_RESULTS, topics.partitions(), topics));
        all.add(topic(Topics.TASK_RESULTS + Topics.DEAD_LETTER_SUFFIX, topics.partitions(), topics));
        all.add(topic(Topics.WORKFLOW_EVENTS, 3, topics));
        all.add(topic(Topics.WORKER_EVENTS, 1, topics));
        return new KafkaAdmin.NewTopics(all.toArray(NewTopic[]::new));
    }

    private static NewTopic topic(String name, int partitions, KafkaTopicsProperties topics) {
        return TopicBuilder.name(name).partitions(partitions).replicas(topics.replicationFactor()).build();
    }

    /**
     * Failed records of the result consumer: retried 3 times, 1 s apart, then published to {@code <topic>.DLT} (same
     * partition) so one bad record can't stop the partition. An unreadable record skips the retries.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
            (record, e) -> new TopicPartition(record.topic() + Topics.DEAD_LETTER_SUFFIX, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3));
        handler.addNotRetryableExceptions(InvalidMessageException.class);
        // The database being unreachable says nothing about the record: retry it (backing off up to 10 s) until the
        // database is back, never dead-letter it. Otherwise a short outage would throw away valid results/commands.
        handler.setBackOffFunction((record, e) -> databaseUnavailable(e) ? untilDatabaseIsBack() : null);
        return handler;
    }

    @Bean
    OutboxRelay outboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction,
                            Clock clock, ObjectMapper json, MeterRegistry meters, OutboxProperties outbox) {
        return new OutboxRelay(jdbc, kafka, transaction, clock, json, meters, outbox.batchSize(), outbox.sendTimeout());
    }

    @Bean
    OutboxJanitor outboxJanitor(JdbcTemplate jdbc, Clock clock, OutboxProperties outbox) {
        return new OutboxJanitor(jdbc, clock, outbox.retention());
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.jobs.enabled", havingValue = "true", matchIfMissing = true)
    RecurringJob outboxRelayJob(OutboxRelay relay, OutboxProperties outbox) {
        return new RecurringJob("outbox-relay", outbox.relayInterval(), relay::relayOnce);
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.jobs.enabled", havingValue = "true", matchIfMissing = true)
    RecurringJob outboxJanitorJob(OutboxJanitor janitor, OutboxProperties outbox) {
        return new RecurringJob("outbox-janitor", outbox.janitorInterval(), janitor::cleanUp);
    }

    /** Spring's own "database unreachable" exceptions, or a JDBC connectivity error anywhere in the chain. */
    private static boolean databaseUnavailable(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof DataAccessResourceFailureException || cause instanceof TransientDataAccessException) {
                return true;
            }
        }
        return DatabaseErrors.isUnavailable(e);
    }

    private static ExponentialBackOff untilDatabaseIsBack() {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(10_000L);   // max elapsed time stays unlimited
        return backOff;
    }
}

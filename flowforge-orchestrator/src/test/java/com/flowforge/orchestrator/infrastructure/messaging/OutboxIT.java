package com.flowforge.orchestrator.infrastructure.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.Topics;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxWriter.OutboxMessage;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository.NewEvent;
import com.flowforge.orchestrator.testing.Definitions;
import com.flowforge.orchestrator.testing.OrchestratorIntegrationTestBase;
import com.flowforge.orchestrator.testing.Runs;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The transactional outbox against real PostgreSQL and Kafka containers. Background jobs are off (test profile), so
 * relays are driven by hand; every test first drains rows other tests left unpublished, so batches contain its own.
 */
class OutboxIT extends OrchestratorIntegrationTestBase {

    private static String token;

    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private TransactionTemplate transaction;
    @Autowired
    private OutboxWriter writer;
    @Autowired
    private EventPublisher publisher;
    @Autowired
    private KafkaTemplate<String, String> kafka;
    @Autowired
    private ObjectMapper mapper;
    @Autowired
    private OutboxRelay relay;
    @Autowired
    private MeterRegistry meters;

    private Runs runs;

    @BeforeEach
    void setUp() {
        token = token == null ? api().newUserToken() : token;
        runs = new Runs(api(), token);
        drain(relay);
    }

    private OutboxRelay newRelay(Runnable beforeCommit) {
        return new OutboxRelay(jdbc, kafka, transaction, Clock.systemUTC(), mapper, new SimpleMeterRegistry(), 100,
            Duration.ofSeconds(3), beforeCommit);
    }

    private static void drain(OutboxRelay relay) {
        await().atMost(Duration.ofSeconds(60)).until(() -> relay.relayOnce() == 0);
    }

    /** Writes {@code count} messages with this key in one transaction; returns their message ids in id order. */
    private List<UUID> write(String key, int count) {
        List<UUID> ids = new ArrayList<>();
        transaction.executeWithoutResult(status -> {
            for (int i = 0; i < count; i++) {
                UUID id = UUID.randomUUID();
                ids.add(id);
                writer.write(new OutboxMessage(id, Topics.WORKFLOW_EVENTS, key, "TestMessage", "{\"n\":" + i + "}",
                    Map.of(Topics.HEADER_CORRELATION_ID, "corr-" + key)), Instant.now());
            }
        });
        return ids;
    }

    private int unpublished(String key) {
        return jdbc.queryForObject("SELECT count(*) FROM outbox WHERE message_key = ? AND published_at IS NULL",
            Integer.class, key);
    }

    /**
     * Every record with this key on {@code ff.workflow.events}, read from the beginning up to the end offsets as of
     * now. Callers only consume after the relay has had its sends acknowledged, so everything sent is in that range,
     * duplicates included.
     */
    private List<ConsumerRecord<String, String>> consume(String key) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<ConsumerRecord<String, String>> mine = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.WORKFLOW_EVENTS).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> {
                    if (key.equals(r.key())) {
                        mine.add(r);
                    }
                });
                return partitions.stream().allMatch(p -> consumer.position(p) >= end.get(p));
            });
        }
        return mine;
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("T14.1 the business transaction commits → timeline row and outbox row both exist (same event id); "
        + "it rolls back → neither exists")
    void sameTransaction() {
        UUID execution = UUID.fromString(runs.run(Definitions.diamond()));
        String committed = "Committed" + UUID.randomUUID().toString().substring(0, 8);
        String rolledBack = "RolledBack" + UUID.randomUUID().toString().substring(0, 8);

        transaction.executeWithoutResult(status -> publisher.publish(execution, "corr",
            List.of(new NewEvent(null, committed, "{}")), Instant.now()));
        transaction.executeWithoutResult(status -> {
            publisher.publish(execution, "corr", List.of(new NewEvent(null, rolledBack, "{}")), Instant.now());
            status.setRollbackOnly();
        });

        UUID eventId = jdbc.queryForObject("SELECT event_id FROM workflow_events WHERE type = ?", UUID.class, committed);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE message_id = ? AND message_type = ?",
            Integer.class, eventId, committed)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM workflow_events WHERE type = ?", Integer.class, rolledBack))
            .isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE message_type = ?", Integer.class, rolledBack))
            .isZero();
    }

    @Test
    @DisplayName("T14.2 the relay publishes in id order with the right key, ff-message-type and ff-correlation-id "
        + "headers, and marks the rows published")
    void publishesInOrder() {
        String key = "order-" + UUID.randomUUID();
        List<UUID> ids = write(key, 5);

        drain(relay);

        assertThat(unpublished(key)).isZero();
        List<ConsumerRecord<String, String>> records = consume(key);
        assertThat(records).extracting(r -> UUID.fromString(header(r, Topics.HEADER_MESSAGE_ID)))
            .containsExactlyElementsOf(ids);
        assertThat(records).allSatisfy(r -> {
            assertThat(header(r, Topics.HEADER_MESSAGE_TYPE)).isEqualTo("TestMessage");
            assertThat(header(r, Topics.HEADER_CORRELATION_ID)).isEqualTo("corr-" + key);
        });
        assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox WHERE message_key = ? AND publish_attempts = 1",
            Integer.class, key)).isEqualTo(5);
    }

    @Test
    @DisplayName("T14.3 Kafka unavailable (container paused) → rows stay unpublished and the API keeps working; Kafka "
        + "back → the backlog drains and outbox.lag.seconds returns to ~0")
    void kafkaOutage() {
        String key = "outage-" + UUID.randomUUID();
        String workflowId = runs.publish(Definitions.diamond());
        // Pauses the Kafka container every IT shares: safe only because ITs run one at a time (never in parallel).
        KAFKA.getDockerClient().pauseContainerCmd(KAFKA.getContainerId()).exec();
        try {
            write(key, 3);
            relay.relayOnce();
            assertThat(unpublished(key)).isEqualTo(3);
            assertThat(api().send(HttpMethod.POST, "/api/v1/workflows/" + workflowId + "/execute", Map.of(), token)
                .getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
            assertThat(meters.get("outbox.lag.seconds").gauge().value()).isPositive();
        } finally {
            KAFKA.getDockerClient().unpauseContainerCmd(KAFKA.getContainerId()).exec();
        }

        await().atMost(Duration.ofSeconds(60)).until(() -> {
            relay.relayOnce();
            return unpublished(key) == 0;
        });
        drain(relay);
        assertThat(meters.get("outbox.lag.seconds").gauge().value()).isLessThan(1.0);
        assertThat(consume(key)).hasSizeGreaterThanOrEqualTo(3);
    }

    @Test
    @DisplayName("T14.4 a crash after send() but before commit → the batch is sent again: delivered twice, and a "
        + "consumer deduplicating by message id sees each once (at-least-once)")
    void crashBeforeCommit() {
        String key = "crash-" + UUID.randomUUID();
        List<UUID> ids = write(key, 2);
        AtomicBoolean crashed = new AtomicBoolean();
        OutboxRelay crashing = newRelay(() -> {
            if (crashed.compareAndSet(false, true)) {
                throw new IllegalStateException("injected crash before commit");
            }
        });

        assertThatThrownBy(crashing::relayOnce).hasMessageContaining("injected crash");
        assertThat(unpublished(key)).isEqualTo(2);
        drain(relay);

        List<ConsumerRecord<String, String>> records = consume(key);
        assertThat(records).hasSize(4);
        Set<UUID> deduplicated = new HashSet<>();
        records.forEach(r -> deduplicated.add(UUID.fromString(header(r, Topics.HEADER_MESSAGE_ID))));
        assertThat(deduplicated).containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    @DisplayName("T14.5 two relays at once, 1 000 messages → every message published exactly once (only one relay is "
        + "active at a time; the other skips)")
    void twoRelays() throws Exception {
        String key = "pair-" + UUID.randomUUID();
        List<UUID> ids = write(key, 1_000);
        OutboxRelay first = newRelay(() -> { });
        OutboxRelay second = newRelay(() -> { });
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Integer>> runs = new ArrayList<>();
            for (OutboxRelay one : List.of(first, second)) {
                runs.add(pool.submit(() -> {
                    go.await();
                    int sent = 0;
                    while (unpublished(key) > 0) {
                        sent += one.relayOnce();
                    }
                    return sent;
                }));
            }
            go.countDown();
            int total = 0;
            for (Future<Integer> run : runs) {
                total += run.get();
            }
            assertThat(total).as("rows marked published by the two relays").isEqualTo(1_000);
        } finally {
            pool.shutdownNow();
        }

        List<ConsumerRecord<String, String>> settled = consume(key);
        assertThat(settled).hasSize(1_000);
        assertThat(settled).extracting(r -> UUID.fromString(header(r, Topics.HEADER_MESSAGE_ID)))
            .containsExactlyInAnyOrderElementsOf(ids);
    }

    @Test
    @DisplayName("T14.6 OutboxWriter outside a transaction → IllegalTransactionStateException")
    void writerNeedsTransaction() {
        OutboxMessage message = new OutboxMessage(UUID.randomUUID(), Topics.WORKFLOW_EVENTS, "k", "TestMessage", "{}",
            Map.of());
        assertThatThrownBy(() -> writer.write(message, Instant.now()))
            .isInstanceOf(IllegalTransactionStateException.class);
    }

    @Test
    @DisplayName("T14.7 the janitor deletes only published rows older than the retention")
    void janitor() {
        Instant now = Instant.now();
        String key = "janitor-" + UUID.randomUUID();
        insertRow(key, now.minus(Duration.ofDays(30)), now.minus(Duration.ofDays(8)));   // old, published → deleted
        insertRow(key, now.minus(Duration.ofDays(30)), now.minus(Duration.ofDays(1)));   // recent → kept
        insertRow(key, now.minus(Duration.ofDays(30)), null);                             // unpublished → kept

        new OutboxJanitor(jdbc, Clock.fixed(now, ZoneOffset.UTC), Duration.ofDays(7)).cleanUp();

        assertThat(jdbc.queryForList("SELECT published_at FROM outbox WHERE message_key = ? ORDER BY id",
            Timestamp.class, key)).hasSize(2).first().satisfies(published ->
            assertThat(published.toInstant()).isAfter(now.minus(Duration.ofDays(2))));
        jdbc.update("DELETE FROM outbox WHERE message_key = ?", key);
    }

    private void insertRow(String key, Instant created, Instant published) {
        jdbc.update("INSERT INTO outbox (message_id, topic, message_key, message_type, payload, created_at, published_at) "
                + "VALUES (?, ?, ?, 'TestMessage', '{}', ?, ?)", UUID.randomUUID(), Topics.WORKFLOW_EVENTS, key,
            Timestamp.from(created), published == null ? null : Timestamp.from(published));
    }

    @Test
    @DisplayName("T14.extra-1 a row that can never be published (record too large) is parked; the rows behind it are "
        + "still published")
    void poisonRowParked() {
        String key = "poison-" + UUID.randomUUID();
        String huge = "{\"x\":\"" + "a".repeat(2_000_000) + "\"}";   // > the producer's max.request.size (1 MB)
        UUID poison = UUID.randomUUID();
        transaction.executeWithoutResult(status -> writer.write(new OutboxMessage(poison, Topics.WORKFLOW_EVENTS, key,
            "TestMessage", huge, Map.of()), Instant.now()));
        List<UUID> behind = write(key, 2);

        drain(relay);

        assertThat(jdbc.queryForObject("SELECT parked_at IS NOT NULL FROM outbox WHERE message_id = ?", Boolean.class,
            poison)).isTrue();
        assertThat(unpublished(key)).isEqualTo(1);   // only the parked one
        assertThat(consume(key)).extracting(r -> UUID.fromString(header(r, Topics.HEADER_MESSAGE_ID)))
            .containsExactlyElementsOf(behind);
        jdbc.update("DELETE FROM outbox WHERE message_id = ?", poison);
    }
}

package com.flowforge.orchestrator.infrastructure.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.Topics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Publishes outbox rows to Kafka, oldest first. One batch is one transaction. Only one relay works at a
 * time (transaction-scoped advisory lock), which keeps per-execution order with several orchestrators; the rows are
 * still taken with {@code FOR UPDATE SKIP LOCKED}, so even without that lock two relays could never send the same
 * rows. Each send waits for the broker's ack before the row is marked published; the first retryable failure stops
 * the batch, so a row is never published after an earlier one that failed. A row that can never be sent is parked.
 *
 * <p>Guarantee: at-least-once. If the process dies after a send but before the commit, the rows are still
 * unpublished and are sent again; consumers deduplicate by message/event id. The error of a failed send is recorded
 * in the same transaction (it commits anyway), not a separate one, because that separate transaction would wait on
 * this transaction's own row lock.
 */
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int MAX_ERROR = 1_000;
    /** Advisory lock key for "the active relay" (any constant; only the relay uses it). */
    private static final long RELAY_LOCK = 0x0F10_F0E0_0001L;

    private record Row(long id, UUID messageId, String topic, String key, String type, String payload,
                       String headers) {
    }

    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate transaction;
    private final Clock clock;
    private final ObjectMapper json;
    private final int batchSize;
    private final Duration sendTimeout;
    private final Runnable beforeCommit;
    private final Counter published;
    private final Counter failures;
    private final Counter parked;

    public OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction,
                       Clock clock, ObjectMapper json, MeterRegistry meters, int batchSize, Duration sendTimeout) {
        this(jdbc, kafka, transaction, clock, json, meters, batchSize, sendTimeout, () -> { });
    }

    /** {@code beforeCommit} runs after the sends, before the commit: tests inject a crash there (T14.4). */
    OutboxRelay(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka, TransactionTemplate transaction, Clock clock,
                ObjectMapper json, MeterRegistry meters, int batchSize, Duration sendTimeout, Runnable beforeCommit) {
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.transaction = transaction;
        this.clock = clock;
        this.json = json;
        this.batchSize = batchSize;
        this.sendTimeout = sendTimeout;
        this.beforeCommit = beforeCommit;
        this.published = Counter.builder("outbox.published").description("Outbox rows published to Kafka")
            .register(meters);
        this.failures = Counter.builder("outbox.publish.failures").description("Failed outbox sends (batch stopped)")
            .register(meters);
        this.parked = Counter.builder("outbox.parked").description("Outbox rows that can never be published (parked)")
            .register(meters);
        Gauge.builder("outbox.lag.seconds", this, OutboxRelay::lagSeconds)
            .description("Age of the oldest unpublished outbox row (0 when none)").register(meters);
    }

    /** One batch; returns how many rows it published. */
    public int relayOnce() {
        Integer count = transaction.execute(status -> {
            // One active relay at a time, cluster-wide: with two, relay B could publish rows 101-200 while A is still
            // on 1-100, breaking per-execution order. The lock ends with the transaction; a second relay just skips.
            if (!Boolean.TRUE.equals(jdbc.queryForObject("SELECT pg_try_advisory_xact_lock(?)", Boolean.class,
                RELAY_LOCK))) {
                return 0;
            }
            List<Row> rows = jdbc.query("""
                    SELECT id, message_id, topic, message_key, message_type, payload::text, headers::text
                    FROM outbox WHERE published_at IS NULL AND parked_at IS NULL ORDER BY id LIMIT ?
                    FOR UPDATE SKIP LOCKED
                    """, (rs, n) -> new Row(rs.getLong(1), rs.getObject(2, UUID.class), rs.getString(3),
                rs.getString(4), rs.getString(5), rs.getString(6), rs.getString(7)), batchSize);
            int sent = 0;
            for (Row row : rows) {
                try {
                    kafka.send(record(row)).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (ExecutionException | TimeoutException | RuntimeException e) {
                    failures.increment();
                    if (retryable(e)) {
                        log.warn("outbox send failed id={} topic={} error={}", row.id(), row.topic(), e.toString());
                        jdbc.update("UPDATE outbox SET publish_attempts = publish_attempts + 1, last_error = ? "
                            + "WHERE id = ?", truncate(e.toString()), row.id());
                        break;   // keep order: nothing after a failed row is published in this batch
                    }
                    // Retrying can't fix it (record too large, bad headers, ...): park it so it can't block the
                    // rows behind it forever. Visible in outbox.parked and the parked_at / last_error columns.
                    parked.increment();
                    log.error("outbox row parked id={} topic={} error={}", row.id(), row.topic(), e.toString());
                    jdbc.update("UPDATE outbox SET publish_attempts = publish_attempts + 1, last_error = ?, "
                        + "parked_at = ? WHERE id = ?", truncate(e.toString()), Timestamp.from(clock.instant()), row.id());
                    continue;
                }
                jdbc.update("UPDATE outbox SET published_at = ?, publish_attempts = publish_attempts + 1 WHERE id = ?",
                    Timestamp.from(clock.instant()), row.id());
                sent++;
            }
            beforeCommit.run();
            return sent;
        });
        int sent = count == null ? 0 : count;
        published.increment(sent);
        return sent;
    }

    private ProducerRecord<String, String> record(Row row) {
        ProducerRecord<String, String> record = new ProducerRecord<>(row.topic(), row.key(), row.payload());
        headers(row.headers()).forEach((name, value) -> record.headers().add(name, utf8(value)));
        record.headers().add(Topics.HEADER_MESSAGE_TYPE, utf8(row.type()));
        record.headers().add(Topics.HEADER_MESSAGE_ID, utf8(row.messageId().toString()));
        return record;
    }

    private Map<String, String> headers(String text) {
        try {
            return json.readValue(text, new TypeReference<Map<String, String>>() { });
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("outbox headers are not a JSON object of strings", e);
        }
    }

    private double lagSeconds() {
        Timestamp oldest = jdbc.queryForObject("SELECT min(created_at) FROM outbox WHERE published_at IS NULL "
            + "AND parked_at IS NULL", Timestamp.class);
        return oldest == null ? 0 : Math.max(0, Duration.between(oldest.toInstant(), clock.instant()).toMillis() / 1000.0);
    }

    /**
     * Retry unless the error is one that retrying can't fix: a record over the size limit, an invalid topic name, a
     * value the client can't serialize, or unreadable stored headers. Everything else (broker down, timeouts, leader
     * changes, whatever wrapper the client puts around them) is retried, so an outage never parks good rows.
     */
    private static boolean retryable(Exception e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof RecordTooLargeException || cause instanceof InvalidTopicException
                || cause instanceof SerializationException || cause instanceof JsonProcessingException) {
                return false;
            }
        }
        return true;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String truncate(String text) {
        return text.length() <= MAX_ERROR ? text : text.substring(0, MAX_ERROR);
    }
}

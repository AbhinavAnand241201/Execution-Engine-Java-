package com.flowforge.orchestrator.infrastructure.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Adds a message to the outbox inside the caller's transaction. That is the whole point: the message
 * exists if and only if the state change it describes committed, so there is no "DB updated but Kafka send failed"
 * (or the reverse) dual write. {@code MANDATORY} makes a call outside a transaction fail loudly instead of silently
 * committing the message on its own.
 */
@Component
public class OutboxWriter {

    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public OutboxWriter(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /** One message to publish later; {@code payload} is JSON text. */
    public record OutboxMessage(UUID messageId, String topic, String key, String type, String payload,
                                Map<String, String> headers) {
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void write(OutboxMessage message, Instant now) {
        ObjectNode headers = json.createObjectNode();
        message.headers().forEach(headers::put);
        jdbc.update("INSERT INTO outbox (message_id, topic, message_key, message_type, payload, headers, created_at) "
                + "VALUES (?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)", message.messageId(), message.topic(), message.key(),
            message.type(), message.payload(), headers.toString(), Timestamp.from(now));
    }
}

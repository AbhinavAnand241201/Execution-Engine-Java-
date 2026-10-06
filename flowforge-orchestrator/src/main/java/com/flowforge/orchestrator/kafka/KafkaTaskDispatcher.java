package com.flowforge.orchestrator.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.Topics;
import com.flowforge.orchestrator.engine.TaskDispatcher;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxWriter;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxWriter.OutboxMessage;
import java.time.Clock;
import java.util.Map;

/**
 * Kafka mode: a command is an outbox row for {@code ff.task.commands.<type>}, keyed by task execution id,
 * written in the dispatch transaction. So the command exists exactly when READY → QUEUED committed, and the
 * outbox relay sends it. The topic per type means only workers with that capability ever receive it.
 */
public class KafkaTaskDispatcher implements TaskDispatcher {

    private final OutboxWriter outbox;
    private final ObjectMapper json;
    private final Clock clock;

    public KafkaTaskDispatcher(OutboxWriter outbox, ObjectMapper json, Clock clock) {
        this.outbox = outbox;
        this.json = json;
        this.clock = clock;
    }

    @Override
    public void dispatch(TaskCommand command) {
        String payload;
        try {
            payload = json.writeValueAsString(command);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("cannot serialize task command", e);
        }
        outbox.write(new OutboxMessage(command.messageId(), Topics.taskCommands(command.type()),
            command.taskExecutionId().toString(), "TaskCommand", payload,
            Map.of(Topics.HEADER_CORRELATION_ID, command.correlationId())), clock.instant());
    }
}

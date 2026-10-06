package com.flowforge.worker.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.runtime.runner.CommandOutcome;
import com.flowforge.runtime.runner.TaskRunner;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Hands each task command to the {@link TaskRunner}. The runner only claims and submits the attempt to its
 * pool, then returns, so the poll loop never blocks on a long task (it waits at most the runner's permit timeout):
 * that keeps the consumer well inside {@code max.poll.interval.ms}.
 *
 * <p>Offsets: ACK (claimed, or a duplicate/stale command that was dropped) → commit. RETRY_LATER (no free permit) →
 * nack: the partition is re-seeked and the command comes back after the pause. The offset is committed only after
 * the claim, so a worker that dies before claiming leaves the command to be redelivered; one that dies after the
 * claim leaves a RUNNING task that the timeout sweeper / worker recovery picks up.
 */
public class CommandListener implements AcknowledgingMessageListener<String, String> {

    static final Duration RETRY_LATER_PAUSE = Duration.ofMillis(500);

    private final TaskRunner runner;
    private final ObjectMapper json;

    public CommandListener(TaskRunner runner, ObjectMapper json) {
        this.runner = runner;
        this.json = json;
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        TaskCommand command;
        try {
            command = json.readValue(record.value(), TaskCommand.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            // Not retryable: the error handler sends it straight to <topic>.DLT.
            throw new InvalidMessageException("unreadable task command at offset " + record.offset(), e);
        }
        try {
            if (runner.onCommand(command) == CommandOutcome.RETRY_LATER) {
                ack.nack(RETRY_LATER_PAUSE);
            } else {
                ack.acknowledge();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            ack.nack(RETRY_LATER_PAUSE);
        }
    }
}

package com.flowforge.orchestrator.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.contracts.Topics;
import com.flowforge.orchestrator.engine.ResultProcessor;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Applies results from {@code ff.task.results} (group {@code ff-orchestrator}). The offset is committed
 * only after {@link ResultProcessor#process} committed its transaction, so a crash in between redelivers the result,
 * and re-applying it is a no-op (the task is no longer RUNNING on that attempt). Results are parsed leniently: a newer
 * worker adding a field during a rolling deploy must not poison the stream. An unreadable record goes to the DLT.
 */
public class ResultConsumer implements AcknowledgingMessageListener<String, String> {

    private final ResultProcessor results;
    private final ObjectMapper json;

    public ResultConsumer(ResultProcessor results, ObjectMapper json) {
        this.results = results;
        this.json = json.copy().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        TaskResultMessage result;
        try {
            result = json.readValue(record.value(), TaskResultMessage.class);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new InvalidMessageException("unreadable task result at offset " + record.offset(), e);
        }
        Header correlation = record.headers().lastHeader(Topics.HEADER_CORRELATION_ID);
        try {
            if (correlation != null) {
                MDC.put("correlationId", new String(correlation.value(), StandardCharsets.UTF_8));
            }
            results.process(result);
            ack.acknowledge();
        } finally {
            MDC.remove("correlationId");
        }
    }
}

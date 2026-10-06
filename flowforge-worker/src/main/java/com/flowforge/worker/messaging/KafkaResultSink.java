package com.flowforge.worker.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.contracts.Topics;
import com.flowforge.runtime.runner.TaskResultSink;
import com.flowforge.worker.registry.WorkerFreeze;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Reports attempt results on {@code ff.task.results}, keyed by workflow execution id so all results of one
 * execution land on one partition, in order. The producer is idempotent with acks=all; {@code publish} waits for the
 * broker's ack and throws on failure, and the TaskRunner retries with backoff. A result sent twice is harmless: the
 * orchestrator applies a result only while the task is RUNNING on that attempt.
 */
public class KafkaResultSink implements TaskResultSink {

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;
    private final Duration sendTimeout;
    private final WorkerFreeze freeze;

    public KafkaResultSink(KafkaTemplate<String, String> kafka, ObjectMapper json, Duration sendTimeout,
                           WorkerFreeze freeze) {
        this.kafka = kafka;
        this.json = json;
        this.sendTimeout = sendTimeout;
        this.freeze = freeze;
    }

    @Override
    public void publish(TaskResultMessage result) throws Exception {
        freeze.awaitThawed();   // a frozen process can't report; see WorkerFreeze
        ProducerRecord<String, String> record = new ProducerRecord<>(Topics.TASK_RESULTS,
            result.workflowExecutionId().toString(), json.writeValueAsString(result));
        record.headers().add(Topics.HEADER_MESSAGE_TYPE, "TaskResult".getBytes(StandardCharsets.UTF_8));
        record.headers().add(Topics.HEADER_MESSAGE_ID, result.messageId().toString().getBytes(StandardCharsets.UTF_8));
        if (result.correlationId() != null) {
            record.headers().add(Topics.HEADER_CORRELATION_ID, result.correlationId().getBytes(StandardCharsets.UTF_8));
        }
        kafka.send(record).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }
}

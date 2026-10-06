package com.flowforge.systemtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.contracts.Topics;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.systemtests.support.Api;
import com.flowforge.systemtests.support.Cluster;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Failure recovery end to end: crashed and frozen (zombie) workers, lost commands, poison commands. A
 * "crash" is the WorkerFreeze switch: no heartbeat, no consuming, results never sent.
 */
class RecoverySystemIT {

    private static final Cluster CLUSTER = new Cluster();

    private Api api;

    @BeforeAll
    static void start() {
        CLUSTER.startOrchestrator();
    }

    @AfterAll
    static void stop() {
        CLUSTER.close();
    }

    @BeforeEach
    void setUp() {
        api = Api.newUser(CLUSTER.baseUrl());
        CLUSTER.startWorker("ra", "DELAY", 2).startWorker("rb", "DELAY", 2);
    }

    @AfterEach
    void stopWorkers() {
        CLUSTER.workerNames().forEach(CLUSTER::stopWorker);
        await().atMost(Duration.ofSeconds(20)).until(() -> jdbc().queryForObject(
            "SELECT count(*) FROM workers WHERE status IN ('ACTIVE', 'DRAINING')", Integer.class) == 0);
    }

    private JdbcTemplate jdbc() {
        return CLUSTER.orchestrator().getBean(JdbcTemplate.class);
    }

    /** One DELAY task with 3 attempts and no backoff. */
    private String runOne(long durationMs) {
        return api.run(Map.of("tasks", List.of(Map.of("key", "job", "type", "DELAY",
            "config", Map.of("durationMs", durationMs), "retry", Map.of("maxAttempts", 3, "backoff", "FIXED",
                "initialDelayMs", 0, "maxDelayMs", 0, "jitter", false)))));
    }

    /** Waits until the task runs; returns the cluster name ("ra"/"rb") of the worker running it. */
    private String runningOn(String execution) {
        await().atMost(Duration.ofSeconds(20)).until(() -> "RUNNING".equals(Api.task(api.execution(execution), "job")
            .get("status").asText()));
        String workerId = Api.task(api.execution(execution), "job").get("workerId").asText();
        return workerId.startsWith("ra-") ? "ra" : "rb";
    }

    private List<String> attempts(String execution) {
        List<String> outcomes = new ArrayList<>();
        api.call("GET", "/api/v1/executions/" + execution + "/tasks/job/attempts", null, 200)
            .forEach(a -> outcomes.add(a.get("attempt").asInt() + ":" + a.get("outcome").asText()));
        return outcomes;
    }

    @Test
    @DisplayName("T17.1 a worker crashes mid-task → attempt 1 WORKER_LOST → the task re-runs on the other worker → "
        + "COMPLETED")
    void workerCrash() {
        String execution = runOne(2_000);
        String victim = runningOn(execution);
        String survivor = victim.equals("ra") ? "rb" : "ra";
        String victimId = CLUSTER.workerId(victim);

        CLUSTER.freeze(victim);

        JsonNode done = api.awaitStatus(execution, Duration.ofSeconds(30), "COMPLETED", "FAILED");
        assertThat(done.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(Api.task(done, "job").get("workerId").asText()).isEqualTo(CLUSTER.workerId(survivor));
        assertThat(attempts(execution)).containsExactly("1:WORKER_LOST", "2:SUCCEEDED");
        assertThat(jdbc().queryForObject("SELECT status FROM workers WHERE id = ?", String.class, victimId))
            .isEqualTo("OFFLINE");
    }

    @Test
    @DisplayName("T17.2 zombie: a frozen worker's task is recovered and completed by the other worker; the zombie "
        + "wakes up and reports success for attempt 1 → rejected as stale; the result stays the survivor's")
    void zombie() {
        String execution = runOne(1_000);
        String zombie = runningOn(execution);
        String survivor = zombie.equals("ra") ? "rb" : "ra";
        MeterRegistry meters = CLUSTER.orchestrator().getBean(MeterRegistry.class);
        double staleBefore = meters.counter("task.results.stale").count();

        CLUSTER.freeze(zombie);
        JsonNode done = api.awaitStatus(execution, Duration.ofSeconds(30), "COMPLETED", "FAILED");
        assertThat(done.get("status").asText()).isEqualTo("COMPLETED");

        CLUSTER.unfreeze(zombie);   // its attempt-1 result (held back while frozen) now goes out, late
        await().atMost(Duration.ofSeconds(15)).until(() -> meters.counter("task.results.stale").count() > staleBefore);

        JsonNode after = api.execution(execution);
        assertThat(Api.task(after, "job").get("attempt").asInt()).isEqualTo(2);
        assertThat(Api.task(after, "job").get("workerId").asText()).isEqualTo(CLUSTER.workerId(survivor));
        assertThat(attempts(execution)).containsExactly("1:WORKER_LOST", "2:SUCCEEDED");
    }

    @Test
    @DisplayName("T17.4 a command that was never published (task QUEUED, no message) → the reconciler re-sends it → "
        + "COMPLETED")
    void lostCommand() {
        String execution = api.run(Map.of("tasks", List.of(Api.delay("a", 2_000), Api.delay("b", 0, "a"))));
        await().atMost(Duration.ofSeconds(10)).until(() -> "RUNNING".equals(Api.task(api.execution(execution), "a")
            .get("status").asText()));
        // As if dispatch had committed but its command was lost: QUEUED, attempt 1, long ago, nothing on Kafka.
        jdbc().update("UPDATE task_executions SET status = 'QUEUED', attempt = 1, queued_at = now() - interval '1 hour' "
            + "WHERE workflow_execution_id = ?::uuid AND task_key = 'b'", execution);

        JsonNode done = api.awaitStatus(execution, Duration.ofSeconds(40), "COMPLETED", "FAILED");
        assertThat(done.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(Api.task(done, "b").get("attempt").asInt()).isEqualTo(1);
    }

    @Test
    @DisplayName("T17.extra-1 an unreadable command → ff.task.commands.delay.DLT (the worker's error handler applies "
        + "to its programmatic containers); later commands still run")
    void poisonCommand() throws Exception {
        String key = "poison-" + UUID.randomUUID();
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.kafkaBootstrap());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(Topics.taskCommands(TaskType.DELAY), key, "{not json")).get();
        }

        await().atMost(Duration.ofSeconds(30)).until(() -> dltKeys(Topics.taskCommands(TaskType.DELAY)
            + Topics.DEAD_LETTER_SUFFIX).contains(key));
        String execution = runOne(0);
        assertThat(api.awaitStatus(execution, Duration.ofSeconds(30), "COMPLETED", "FAILED").get("status").asText())
            .isEqualTo("COMPLETED");
    }

    private List<String> dltKeys(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.kafkaBootstrap());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sys-dlt-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<String> keys = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                .map(p -> new TopicPartition(p.topic(), p.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            await().atMost(Duration.ofSeconds(20)).until(() -> {
                consumer.poll(Duration.ofMillis(200)).forEach(r -> keys.add(r.key()));
                return partitions.stream().allMatch(p -> consumer.position(p) >= end.get(p));
            });
        }
        return keys;
    }
}

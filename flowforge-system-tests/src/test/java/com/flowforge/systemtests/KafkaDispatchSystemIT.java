package com.flowforge.systemtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flowforge.core.contracts.TaskCommand;
import com.flowforge.core.contracts.TaskResultMessage;
import com.flowforge.core.contracts.Topics;
import com.flowforge.core.state.AttemptOutcome;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.runner.TaskRunner;
import com.flowforge.systemtests.support.Api;
import com.flowforge.systemtests.support.Cluster;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Kafka dispatch end to end: the orchestrator in Kafka mode and two workers, as separate Spring apps in one
 * JVM, against real PostgreSQL and Kafka. Some tests write messages straight to Kafka to prove the consumers are
 * idempotent, the way a redelivery would.
 */
class KafkaDispatchSystemIT {

    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    private static final Cluster CLUSTER = new Cluster();
    private static KafkaProducer<String, String> producer;

    private Api api;

    @BeforeAll
    static void start() {
        CLUSTER.startOrchestrator().startWorker("w1", "HTTP,DELAY,DATABASE,REPORT", 2)
            .startWorker("w2", "HTTP,DELAY,DATABASE,REPORT", 2);
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.kafkaBootstrap());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(properties);
    }

    @AfterAll
    static void stop() {
        producer.close();
        CLUSTER.close();
    }

    @BeforeEach
    void setUp() {
        api = Api.newUser(CLUSTER.baseUrl());
    }

    private JdbcTemplate jdbc() {
        return CLUSTER.orchestrator().getBean(JdbcTemplate.class);
    }

    private long staleClaims() {
        return CLUSTER.worker("w1").getBean(TaskRunner.class).duplicateOrStaleClaims()
            + CLUSTER.worker("w2").getBean(TaskRunner.class).duplicateOrStaleClaims();
    }

    private void send(String topic, String key, Object value) throws Exception {
        String payload = value instanceof String text ? text : JSON.writeValueAsString(value);
        producer.send(new ProducerRecord<>(topic, key, payload)).get();
    }

    private UUID taskId(String execution, String key) {
        return jdbc().queryForObject("SELECT id FROM task_executions WHERE workflow_execution_id = ?::uuid "
            + "AND task_key = ?", UUID.class, execution, key);
    }

    private int attempts(UUID taskId) {
        return jdbc().queryForObject("SELECT count(*) FROM task_attempts WHERE task_execution_id = ?", Integer.class,
            taskId);
    }

    /** An execution whose task {@code b} waits behind a slow {@code a}, so the test can drive {@code b} by hand. */
    private String slowThenB() {
        String id = api.run(Map.of("tasks", List.of(Api.delay("a", 3_000), Api.delay("b", 0, "a"))));
        await().atMost(Duration.ofSeconds(10)).until(() -> "RUNNING".equals(Api.task(api.execution(id), "a")
            .get("status").asText()));
        return id;
    }

    private TaskCommand command(String execution, UUID taskId, int attempt) {
        return new TaskCommand(UUID.randomUUID(), TaskCommand.SCHEMA_VERSION, taskId, UUID.fromString(execution), "b",
            TaskType.DELAY, attempt, JSON.createObjectNode().put("durationMs", 0), List.of("a"), 60_000, "sys-corr",
            Instant.now());
    }

    @Test
    @DisplayName("T15.1 a diamond completes through Kafka with 2 workers; a 20-task fan-out runs on both workers")
    void diamondAndFanOut() {
        String diamond = api.run(Map.of("tasks", List.of(Api.delay("a", 0), Api.delay("b", 100, "a"),
            Api.delay("c", 100, "a"), Api.delay("d", 100, "a"), Api.delay("e", 0, "b", "c", "d"))));
        assertThat(api.awaitStatus(diamond, Duration.ofSeconds(30), "COMPLETED", "FAILED").get("status").asText())
            .isEqualTo("COMPLETED");

        List<Object> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            tasks.add(Api.delay("t" + i, 200));
        }
        String fanOut = api.run(Map.of("tasks", tasks));
        JsonNode execution = api.awaitStatus(fanOut, Duration.ofSeconds(60), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        Set<String> workerNames = new HashSet<>();
        execution.get("tasks").forEach(t -> workerNames.add(t.get("workerId").asText().replaceAll("-[0-9a-f]{8}$", "")));
        assertThat(workerNames).containsExactlyInAnyOrder("w1", "w2");
    }

    @Test
    @DisplayName("T15.3 the same command record produced twice straight to Kafka → the executor runs once")
    void duplicateCommand() throws Exception {
        String execution = slowThenB();
        UUID b = taskId(execution, "b");
        jdbc().update("UPDATE task_executions SET status = 'QUEUED', attempt = 1, queued_at = now() WHERE id = ?", b);
        long staleBefore = staleClaims();
        TaskCommand command = command(execution, b, 1);

        send(Topics.taskCommands(TaskType.DELAY), b.toString(), command);
        send(Topics.taskCommands(TaskType.DELAY), b.toString(), command);

        assertThat(api.awaitStatus(execution, Duration.ofSeconds(30), "COMPLETED", "FAILED").get("status").asText())
            .isEqualTo("COMPLETED");
        await().atMost(Duration.ofSeconds(10)).until(() -> staleClaims() - staleBefore >= 1);
        assertThat(attempts(b)).isEqualTo(1);
    }

    @Test
    @DisplayName("T15.4 the same result record produced twice → applied once (the second is stale)")
    void duplicateResult() throws Exception {
        String execution = slowThenB();
        UUID b = taskId(execution, "b");
        jdbc().update("UPDATE task_executions SET status = 'RUNNING', attempt = 1, worker_id = 'sys-fake', "
            + "started_at = now() WHERE id = ?", b);
        MeterRegistry meters = CLUSTER.orchestrator().getBean(MeterRegistry.class);
        double staleBefore = meters.counter("task.results.stale").count();
        Instant now = Instant.now();
        TaskResultMessage result = new TaskResultMessage(UUID.randomUUID(), 1, b, UUID.fromString(execution), 1,
            "sys-fake", AttemptOutcome.SUCCEEDED, JSON.createObjectNode(), null, null, false, now, now, "sys-corr");

        send(Topics.TASK_RESULTS, execution, result);
        send(Topics.TASK_RESULTS, execution, result);

        await().atMost(Duration.ofSeconds(15)).until(() -> meters.counter("task.results.stale").count() - staleBefore >= 1);
        assertThat(Api.task(api.execution(execution), "b").get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(attempts(b)).isEqualTo(1);
    }

    @Test
    @DisplayName("T15.5 invalid JSON on ff.task.results → ff.task.results.DLT; the next valid result is still applied")
    void poisonResult() throws Exception {
        String execution = slowThenB();
        UUID b = taskId(execution, "b");
        jdbc().update("UPDATE task_executions SET status = 'RUNNING', attempt = 1, worker_id = 'sys-fake', "
            + "started_at = now() WHERE id = ?", b);
        String poisonKey = "poison-" + UUID.randomUUID();
        Instant now = Instant.now();

        send(Topics.TASK_RESULTS, poisonKey, "{not json");
        send(Topics.TASK_RESULTS, execution, new TaskResultMessage(UUID.randomUUID(), 1, b, UUID.fromString(execution),
            1, "sys-fake", AttemptOutcome.SUCCEEDED, JSON.createObjectNode(), null, null, false, now, now, "c"));

        await().atMost(Duration.ofSeconds(20)).until(() -> "SUCCEEDED".equals(Api.task(api.execution(execution), "b")
            .get("status").asText()));
        // The poison record sits on another partition, handled independently of the valid one: wait for it too.
        await().atMost(Duration.ofSeconds(30)).until(() -> dltKeys().contains(poisonKey));
    }

    private List<String> dltKeys() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.kafkaBootstrap());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "sys-dlt-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        List<String> keys = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.TASK_RESULTS + Topics.DEAD_LETTER_SUFFIX)
                .stream().map(p -> new TopicPartition(p.topic(), p.partition())).toList();
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

    @Test
    @DisplayName("T15.6 a command for a task that is already CANCELLED → acked and dropped; the executor never runs")
    void commandForCancelledTask() throws Exception {
        String execution = slowThenB();
        UUID b = taskId(execution, "b");
        jdbc().update("UPDATE task_executions SET status = 'CANCELLED', attempt = 1 WHERE id = ?", b);
        long staleBefore = staleClaims();

        send(Topics.taskCommands(TaskType.DELAY), b.toString(), command(execution, b, 1));

        await().atMost(Duration.ofSeconds(15)).until(() -> staleClaims() - staleBefore >= 1);
        assertThat(attempts(b)).isZero();
        assertThat(Api.task(api.execution(execution), "b").get("status").asText()).isEqualTo("CANCELLED");
    }

    @Test
    @DisplayName("T15.8 workers at capacity (2 each) with 12 tasks → excess commands are nacked and run later; "
        + "every task succeeds on attempt 1")
    void atCapacity() {
        List<Object> tasks = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            tasks.add(Api.delay("t" + i, 500));
        }
        String id = api.run(Map.of("tasks", tasks));

        JsonNode execution = api.awaitStatus(id, Duration.ofSeconds(60), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        execution.get("tasks").forEach(t -> assertThat(t.get("attempt").asInt()).isEqualTo(1));
    }

    @Test
    @DisplayName("T15.7 the orchestrator restarts mid-workflow → the execution still completes (state in PostgreSQL, "
        + "results in Kafka, offsets committed only after processing)")
    void orchestratorRestart() {
        String id = api.run(Map.of("tasks", List.of(Api.delay("a", 2_000), Api.delay("b", 0, "a"))));
        await().atMost(Duration.ofSeconds(10)).until(() -> "RUNNING".equals(Api.task(api.execution(id), "a")
            .get("status").asText()));

        CLUSTER.stopOrchestrator();
        CLUSTER.startOrchestrator();
        Api restarted = api.at(CLUSTER.baseUrl());

        assertThat(restarted.awaitStatus(id, Duration.ofSeconds(60), "COMPLETED", "FAILED").get("status").asText())
            .isEqualTo("COMPLETED");
    }
}

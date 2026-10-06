package com.flowforge.systemtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.RedisKeys;
import com.flowforge.systemtests.support.Api;
import com.flowforge.systemtests.support.Cluster;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Worker registry, heartbeats, drain and graceful shutdown, plus capability routing (T15.2, which needs
 * the registry). One orchestrator for the class; every test starts its own workers and stops them afterwards.
 */
class WorkerRegistrySystemIT {

    private static final ObjectMapper JSON = new ObjectMapper();
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
    }

    /** Stops this test's workers and waits until the registry has no live worker left (frozen ones expire). */
    @AfterEach
    void stopWorkers() {
        CLUSTER.workerNames().forEach(CLUSTER::stopWorker);
        await().atMost(Duration.ofSeconds(20)).until(() -> jdbc().queryForObject(
            "SELECT count(*) FROM workers WHERE status IN ('ACTIVE', 'DRAINING')", Integer.class) == 0);
    }

    private JdbcTemplate jdbc() {
        return CLUSTER.orchestrator().getBean(JdbcTemplate.class);
    }

    private StringRedisTemplate redis() {
        return CLUSTER.orchestrator().getBean(StringRedisTemplate.class);
    }

    private String workerStatus(String id) {
        return jdbc().queryForObject("SELECT status FROM workers WHERE id = ?", String.class, id);
    }

    private Api operator() {
        return Api.login(CLUSTER.baseUrl(), Cluster.OPERATOR_EMAIL, Cluster.OPERATOR_PASSWORD);
    }

    private String runningTaskWorker(String execution, String key) {
        await().atMost(Duration.ofSeconds(20)).until(() -> "RUNNING".equals(Api.task(api.execution(execution), key)
            .get("status").asText()));
        return Api.task(api.execution(execution), key).get("workerId").asText();
    }

    @Test
    @DisplayName("T16.1 a worker registers ACTIVE with its capabilities; WorkerRegistered goes to the outbox")
    void registers() {
        CLUSTER.startWorker("reg", "DELAY,REPORT", 3);
        String id = CLUSTER.workerId("reg");

        assertThat(id).matches("reg-[0-9a-f]{8}");
        Map<String, Object> row = jdbc().queryForMap("SELECT status, capabilities::text AS caps, max_concurrency "
            + "FROM workers WHERE id = ?", id);
        assertThat(row).containsEntry("status", "ACTIVE").containsEntry("caps", "{DELAY,REPORT}")
            .containsEntry("max_concurrency", 3);
        assertThat(jdbc().queryForObject("SELECT count(*) FROM outbox WHERE message_key = ? AND message_type = "
            + "'WorkerRegistered' AND topic = 'ff.worker.events'", Integer.class, id)).isEqualTo(1);
        JsonNode view = operator().call("GET", "/api/v1/workers/" + id, null, 200);
        assertThat(view.get("status").asText()).isEqualTo("ACTIVE");
    }

    @Test
    @DisplayName("T16.2 the heartbeat key exists with TTL ≤ the configured TTL and the expected payload")
    void heartbeat() throws Exception {
        CLUSTER.startWorker("hb", "DELAY", 2);
        String key = RedisKeys.heartbeat(CLUSTER.workerId("hb"));

        await().atMost(Duration.ofSeconds(5)).until(() -> redis().hasKey(key));
        Long ttl = redis().getExpire(key, TimeUnit.MILLISECONDS);
        assertThat(ttl).isPositive().isLessThanOrEqualTo(3_000);
        JsonNode payload = JSON.readTree(redis().opsForValue().get(key));
        assertThat(payload.get("load").asInt()).isZero();
        assertThat(payload.get("maxConcurrency").asInt()).isEqualTo(2);
        assertThat(payload.get("capabilities")).extracting(JsonNode::asText).containsExactly("DELAY");
        assertThat(payload.hasNonNull("ts")).isTrue();
    }

    @Test
    @DisplayName("T16.3 heartbeat stops (frozen worker) → OFFLINE within TTL + one monitor interval")
    void silentWorkerGoesOffline() {
        CLUSTER.startWorker("silent", "DELAY", 2);
        String id = CLUSTER.workerId("silent");
        await().atMost(Duration.ofSeconds(5)).until(() -> redis().hasKey(RedisKeys.heartbeat(id)));

        long frozenAt = System.nanoTime();
        CLUSTER.freeze("silent");

        await().atMost(Duration.ofSeconds(10)).until(() -> "OFFLINE".equals(workerStatus(id)));
        assertThat(Duration.ofNanos(System.nanoTime() - frozenAt)).isLessThan(Duration.ofMillis(3_000 + 500 + 2_000));
    }

    @Test
    @DisplayName("T16.5 drain → new tasks go to the other worker; the in-flight task completes; the drained worker is "
        + "DRAINING with load 0")
    void drain() {
        CLUSTER.startWorker("da", "DELAY", 2).startWorker("db", "DELAY", 2);
        String slow = api.run(Map.of("tasks", List.of(Api.delay("slow", 2_000))));
        String busyWorker = runningTaskWorker(slow, "slow");
        String other = busyWorker.startsWith("da-") ? CLUSTER.workerId("db") : CLUSTER.workerId("da");

        assertThat(operator().call("POST", "/api/v1/workers/" + busyWorker + "/drain", null, 202).get("status")
            .asText()).isEqualTo("DRAINING");
        List<Object> tasks = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            tasks.add(Api.delay("t" + i, 50));
        }
        String busyName = busyWorker.startsWith("da-") ? "da" : "db";
        // The worker sees the flag on its next heartbeat and leaves the consumer groups (partitions move to the other).
        await().atMost(Duration.ofSeconds(5)).until(() -> CLUSTER.consumers(busyName).isDraining());
        String afterDrain = api.run(Map.of("tasks", tasks));

        assertThat(api.awaitStatus(slow, Duration.ofSeconds(20), "COMPLETED", "FAILED").get("status").asText())
            .isEqualTo("COMPLETED");
        JsonNode execution = api.awaitStatus(afterDrain, Duration.ofSeconds(30), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        execution.get("tasks").forEach(t -> assertThat(t.get("workerId").asText()).isEqualTo(other));
        await().atMost(Duration.ofSeconds(5)).until(() -> jdbc().queryForObject(
            "SELECT current_load FROM workers WHERE id = ?", Integer.class, busyWorker) == 0);
        assertThat(workerStatus(busyWorker)).isEqualTo("DRAINING");
    }

    @Test
    @DisplayName("T16.6 shutdown: a task shorter than the grace completes; a longer one is reported WORKER_SHUTDOWN "
        + "and retried on another worker; the worker is OFFLINE and its heartbeat key is gone")
    void gracefulShutdown() {
        CLUSTER.startWorker("short", "DELAY", 2, "flowforge.worker.shutdown-grace", "3s");
        String quick = api.run(Map.of("tasks", List.of(Api.delay("quick", 1_000))));
        runningTaskWorker(quick, "quick");
        CLUSTER.stopWorker("short");
        JsonNode done = api.awaitStatus(quick, Duration.ofSeconds(15), "COMPLETED", "FAILED");
        assertThat(Api.task(done, "quick").get("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(Api.task(done, "quick").get("attempt").asInt()).isEqualTo(1);

        CLUSTER.startWorker("leaving", "DELAY", 2, "flowforge.worker.shutdown-grace", "1s");
        String leaving = CLUSTER.workerId("leaving");
        String slow = api.run(Map.of("tasks", List.of(Map.of("key", "slow", "type", "DELAY",
            "config", Map.of("durationMs", 4_000), "retry", Map.of("maxAttempts", 3, "backoff", "FIXED",
                "initialDelayMs", 0, "maxDelayMs", 0, "jitter", false)))));
        assertThat(runningTaskWorker(slow, "slow")).isEqualTo(leaving);
        CLUSTER.startWorker("taker", "DELAY", 2);
        CLUSTER.stopWorker("leaving");

        assertThat(workerStatus(leaving)).isEqualTo("OFFLINE");
        assertThat(redis().hasKey(RedisKeys.heartbeat(leaving))).isFalse();
        JsonNode execution = api.awaitStatus(slow, Duration.ofSeconds(30), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(Api.task(execution, "slow").get("workerId").asText()).isEqualTo(CLUSTER.workerId("taker"));
        JsonNode attempts = api.call("GET", "/api/v1/executions/" + slow + "/tasks/slow/attempts", null, 200);
        assertThat(attempts.get(0).get("outcome").asText()).isEqualTo("WORKER_SHUTDOWN");
        assertThat(attempts.get(attempts.size() - 1).get("outcome").asText()).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("T16.7 a restart is a new incarnation: new worker id, the old id is OFFLINE")
    void restartIsNewIncarnation() {
        CLUSTER.startWorker("again", "DELAY", 2);
        String first = CLUSTER.workerId("again");
        CLUSTER.stopWorker("again");
        CLUSTER.startWorker("again", "DELAY", 2);
        String second = CLUSTER.workerId("again");

        assertThat(second).startsWith("again-").isNotEqualTo(first);
        assertThat(workerStatus(first)).isEqualTo("OFFLINE");
        assertThat(workerStatus(second)).isEqualTo("ACTIVE");
    }

    @Test
    @DisplayName("T15.2 capability routing: with no HTTP-capable worker an HTTP task stays READY and "
        + "queue.depth{READY,HTTP} > 0; an HTTP worker starts → the execution completes")
    void capabilityRouting() {
        CLUSTER.startWorker("delay-only", "DELAY", 2);
        String url = CLUSTER.baseUrl() + "/actuator/health/liveness";
        String id = api.run(Map.of("tasks", List.of(Map.of("key", "call", "type", "HTTP",
            "config", Map.of("method", "GET", "url", url)))));
        MeterRegistry meters = CLUSTER.orchestrator().getBean(MeterRegistry.class);

        await().atMost(Duration.ofSeconds(10)).until(() -> meters.get("queue.depth").tag("status", "READY")
            .tag("type", "HTTP").gauge().value() > 0);
        assertThat(Api.task(api.execution(id), "call").get("status").asText()).isEqualTo("READY");

        CLUSTER.startWorker("http", "HTTP", 2);
        JsonNode execution = api.awaitStatus(id, Duration.ofSeconds(30), "COMPLETED", "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        assertThat(Api.task(execution, "call").get("workerId").asText()).isEqualTo(CLUSTER.workerId("http"));
    }

    @Test
    @DisplayName("T16.extra-1 a live worker that finds itself declared OFFLINE (it missed its heartbeats, e.g. a long "
        + "GC pause) stops consuming; new tasks run on the other worker")
    void declaredOfflineStopsWorking() {
        CLUSTER.startWorker("ghost", "DELAY", 2).startWorker("alive", "DELAY", 2);
        String ghost = CLUSTER.workerId("ghost");
        // What the monitor does after a missed TTL: the registry now says OFFLINE while the process still runs.
        jdbc().update("UPDATE workers SET status = 'OFFLINE', offline_at = now() WHERE id = ?", ghost);

        await().atMost(Duration.ofSeconds(10)).until(() -> CLUSTER.consumers("ghost").isDraining());
        List<Object> tasks = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            tasks.add(Api.delay("t" + i, 50));
        }
        JsonNode execution = api.awaitStatus(api.run(Map.of("tasks", tasks)), Duration.ofSeconds(30), "COMPLETED",
            "FAILED");
        assertThat(execution.get("status").asText()).isEqualTo("COMPLETED");
        execution.get("tasks").forEach(t -> assertThat(t.get("workerId").asText()).isEqualTo(CLUSTER.workerId("alive")));
    }
}

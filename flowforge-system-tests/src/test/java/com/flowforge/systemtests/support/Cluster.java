package com.flowforge.systemtests.support;

import com.flowforge.orchestrator.FlowForgeOrchestratorApplication;
import com.flowforge.runtime.testing.IntegrationTestBase;
import com.flowforge.worker.FlowForgeWorkerApplication;
import com.flowforge.worker.config.WorkerIdentity;
import com.flowforge.worker.messaging.CommandConsumers;
import com.flowforge.worker.registry.WorkerFreeze;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * The orchestrator (Kafka mode) and workers booted as separate Spring applications in this JVM, against the shared
 * Testcontainers PostgreSQL and Kafka (system tests). Each app has its own context, port and config file
 * ({@code application.yml} vs {@code worker.yml}), like separate processes, but starts in seconds.
 */
public final class Cluster extends IntegrationTestBase implements AutoCloseable {

    /** Test-only values; real environments set FLOWFORGE_JWT_SECRET etc. */
    public static final String ADMIN_EMAIL = "admin@system.flowforge.local";
    public static final String ADMIN_PASSWORD = "system-admin-password";
    public static final String OPERATOR_EMAIL = "operator@system.flowforge.local";
    public static final String OPERATOR_PASSWORD = "system-operator-password";
    private static final String JWT_SECRET = "system-test-only-jwt-secret-never-use-elsewhere-0123456789-padded-64";

    private ConfigurableApplicationContext orchestrator;
    private final Map<String, ConfigurableApplicationContext> workers = new HashMap<>();
    private final List<String> workerOrder = new ArrayList<>();

    public static String kafkaBootstrap() {
        return KAFKA.getBootstrapServers();
    }

    public static String jdbcUrl() {
        return POSTGRES.getJdbcUrl();
    }

    public static String dbUser() {
        return POSTGRES.getUsername();
    }

    public static String dbPassword() {
        return POSTGRES.getPassword();
    }

    public Cluster startOrchestrator(String... extra) {
        Map<String, Object> properties = new HashMap<>(common());
        properties.put("flowforge.dispatch.mode", "kafka");
        properties.put("flowforge.dispatch.poll-interval", "50ms");
        properties.put("flowforge.workers.heartbeat-ttl", "3s");
        properties.put("flowforge.workers.monitor-interval", "500ms");
        properties.put("flowforge.workers.queue-depth-interval", "500ms");
        properties.put("flowforge.outbox.relay-interval", "20ms");
        properties.put("flowforge.retry.promoter-interval", "100ms");
        properties.put("flowforge.timeout.sweep-interval", "500ms");
        properties.put("flowforge.timeout.grace", "2s");
        properties.put("flowforge.reconciler.interval", "1s");
        properties.put("flowforge.reconciler.stale-queued-after", "20s");
        properties.put("flowforge.security.jwt.secret", JWT_SECRET);
        properties.put("flowforge.security.admin.email", ADMIN_EMAIL);
        properties.put("flowforge.security.admin.password", ADMIN_PASSWORD);
        properties.put("flowforge.security.operator.email", OPERATOR_EMAIL);
        properties.put("flowforge.security.operator.password", OPERATOR_PASSWORD);
        properties.putAll(pairs(extra));
        orchestrator = new SpringApplicationBuilder(FlowForgeOrchestratorApplication.class).run(args(properties));
        return this;
    }

    public Cluster startWorker(String name, String capabilities, int maxConcurrency, String... extra) {
        Map<String, Object> properties = new HashMap<>(common());
        properties.put("flowforge.worker.name", name);
        properties.put("flowforge.worker.capabilities", capabilities);
        properties.put("flowforge.worker.max-concurrency", String.valueOf(maxConcurrency));
        properties.put("flowforge.worker.shutdown-grace", "5s");
        properties.put("flowforge.worker.heartbeat-interval", "500ms");
        properties.put("flowforge.worker.heartbeat-ttl", "3s");
        properties.put("flowforge.worker.exit-when-declared-offline", "false");   // never System.exit the test JVM
        properties.putAll(pairs(extra));
        // spring.config.name must be a default property: it decides which file is read before anything else.
        workers.put(name, new SpringApplicationBuilder(FlowForgeWorkerApplication.class)
            .properties("spring.config.name=" + FlowForgeWorkerApplication.CONFIG_NAME).run(args(properties)));
        workerOrder.add(name);
        return this;
    }

    private static Map<String, Object> common() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("server.port", "0");
        properties.put("spring.main.banner-mode", "off");
        properties.put("spring.datasource.url", POSTGRES.getJdbcUrl());
        properties.put("spring.datasource.username", POSTGRES.getUsername());
        properties.put("spring.datasource.password", POSTGRES.getPassword());
        properties.put("spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers());
        properties.put("spring.data.redis.host", REDIS.getHost());
        properties.put("spring.data.redis.port", String.valueOf(REDIS.getMappedPort(6379)));
        properties.put("logging.level.root", "WARN");
        properties.put("logging.level.com.flowforge", "INFO");
        return properties;
    }

    /** As command-line arguments: they win over the apps' own YAML (builder "properties" are only defaults). */
    private static String[] args(Map<String, Object> properties) {
        return properties.entrySet().stream().map(e -> "--" + e.getKey() + "=" + e.getValue()).toArray(String[]::new);
    }

    private static Map<String, Object> pairs(String... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }

    public ConfigurableApplicationContext orchestrator() {
        return orchestrator;
    }

    public ConfigurableApplicationContext worker(String name) {
        return workers.get(name);
    }

    public String baseUrl() {
        return "http://localhost:" + orchestrator.getEnvironment().getProperty("local.server.port");
    }

    public String workerId(String name) {
        return workers.get(name).getBean(WorkerIdentity.class).id();
    }

    /** Simulated freeze/crash: no heartbeat, no new commands, finished attempts can't report (see WorkerFreeze). */
    public void freeze(String name) {
        workers.get(name).getBean(WorkerFreeze.class).freeze();
        workers.get(name).getBean(CommandConsumers.class).drain();
    }

    /** A frozen worker wakes up: it is now a zombie whose late results must be rejected. */
    public void unfreeze(String name) {
        workers.get(name).getBean(WorkerFreeze.class).unfreeze();
    }

    public CommandConsumers consumers(String name) {
        return workers.get(name).getBean(CommandConsumers.class);
    }

    public List<String> workerNames() {
        return List.copyOf(workerOrder);
    }

    public void stopOrchestrator() {
        if (orchestrator != null) {
            orchestrator.close();
            orchestrator = null;
        }
    }

    public void stopWorker(String name) {
        ConfigurableApplicationContext worker = workers.remove(name);
        workerOrder.remove(name);
        if (worker != null) {
            worker.close();
        }
    }

    @Override
    public void close() {
        new ArrayList<>(workerOrder).forEach(this::stopWorker);
        stopOrchestrator();
    }
}

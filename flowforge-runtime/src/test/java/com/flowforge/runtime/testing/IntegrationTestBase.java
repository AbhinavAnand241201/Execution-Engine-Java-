package com.flowforge.runtime.testing;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

/**
 * Starts PostgreSQL, Redis and Kafka once per JVM and shares them across every integration test.
 * Starting containers per test class would multiply the suite's run time; tests stay independent by using
 * unique names and IDs instead. Ryuk removes the containers when the JVM exits.
 *
 * <p>Image tags match {@code docker-compose.yml} so tests run against the same versions as the stack;
 * change both together.
 */
public abstract class IntegrationTestBase {

    public static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse("postgres:16.15-alpine");
    public static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7.4.11-alpine");
    public static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("apache/kafka:3.9.2");

    protected static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(POSTGRES_IMAGE);
    protected static final GenericContainer<?> REDIS = new GenericContainer<>(REDIS_IMAGE).withExposedPorts(6379);
    // Same as docker-compose.yml: topics must be created explicitly (the orchestrator does it).
    protected static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE)
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    static {
        Startables.deepStart(POSTGRES, REDIS, KAFKA).join();
    }

    protected static String redisUri() {
        return "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }
}

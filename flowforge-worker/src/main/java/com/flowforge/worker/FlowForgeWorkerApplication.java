package com.flowforge.worker;

import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Entry point of a worker: consumes task commands, claims and executes tasks, reports results. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FlowForgeWorkerApplication {

    /** {@code worker.yml}, not {@code application.yml}: system tests run a worker and the orchestrator in one JVM. */
    public static final String CONFIG_NAME = "worker";

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(FlowForgeWorkerApplication.class);
        app.setDefaultProperties(Map.of("spring.config.name", CONFIG_NAME));
        app.run(args);
    }
}

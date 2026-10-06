package com.flowforge.orchestrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Entry point of the orchestrator: API, persistence, dispatch, result processing and background jobs. */
@SpringBootApplication
@ConfigurationPropertiesScan
public class FlowForgeOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlowForgeOrchestratorApplication.class, args);
    }
}

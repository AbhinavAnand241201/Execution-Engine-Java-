package com.flowforge.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.dag.DagValidator;
import com.flowforge.core.executor.DelayTaskExecutor;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.core.workflow.WorkflowDefinitionParser;
import com.flowforge.core.workflow.WorkflowDefinitionValidator;
import com.flowforge.orchestrator.application.DefinitionValidation;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.application.ExecutionService;
import com.flowforge.orchestrator.application.PublishedDagCache;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.application.RequestHasher;
import com.flowforge.orchestrator.application.WorkflowService;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowRepository;
import com.flowforge.orchestrator.infrastructure.persistence.WorkflowVersionRepository;
import java.time.Clock;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Wires the pure-Java core into the app. Every {@link TaskExecutor} bean joins the registry, which is also the
 * validator's config hook; the HTTP, DATABASE and REPORT executors come from {@link ExecutorConfig}.
 */
@Configuration
public class WorkflowConfig {

    static final long DAG_CACHE_SIZE = 1_000;

    @Bean
    DelayTaskExecutor delayTaskExecutor() {
        return new DelayTaskExecutor();
    }

    @Bean
    TaskExecutorRegistry taskExecutorRegistry(List<TaskExecutor> executors) {
        return new TaskExecutorRegistry(executors);
    }

    @Bean
    WorkflowDefinitionParser workflowDefinitionParser() {
        return new WorkflowDefinitionParser();
    }

    @Bean
    DefinitionValidation definitionValidation(WorkflowDefinitionParser parser, TaskExecutorRegistry registry,
                                              LimitsProperties limits) {
        return new DefinitionValidation(parser,
            new DagValidator(new WorkflowDefinitionValidator(registry, limits.maxTasksPerWorkflow())));
    }

    @Bean
    PublishedDagCache publishedDagCache(WorkflowService workflows) {
        return new PublishedDagCache(workflows::publishedDefinition, DAG_CACHE_SIZE);
    }

    @Bean
    RequestHasher requestHasher(ObjectMapper json) {
        return new RequestHasher(json);
    }

    @Bean
    ExecutionService executionService(WorkflowRepository workflows, WorkflowVersionRepository versions,
                                      ExecutionJdbcRepository executions, EventPublisher publisher,
                                      PublishedDagCache dags, RequestHasher hasher, ObjectMapper json,
                                      TransactionTemplate transaction, Clock clock, LimitsProperties limits,
                                      ReadyTaskSignal readyTasks) {
        return new ExecutionService(workflows, versions, executions, publisher, dags, hasher, json, transaction, clock,
            limits.inputMaxBytes(), readyTasks);
    }
}

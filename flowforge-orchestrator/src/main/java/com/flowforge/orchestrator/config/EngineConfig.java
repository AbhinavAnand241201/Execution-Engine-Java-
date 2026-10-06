package com.flowforge.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.orchestrator.application.WorkerService;
import com.flowforge.orchestrator.engine.QueueDepthSampler;
import com.flowforge.orchestrator.engine.RecoveryService;
import com.flowforge.orchestrator.engine.WorkerMonitor;
import com.flowforge.orchestrator.infrastructure.persistence.WorkerJdbcRepository;
import com.flowforge.orchestrator.infrastructure.redis.WorkerHeartbeats;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import com.flowforge.orchestrator.application.DeadLetterService;
import com.flowforge.orchestrator.application.EventPublisher;
import com.flowforge.orchestrator.application.PublishedDagCache;
import com.flowforge.orchestrator.application.ReadyTaskSignal;
import com.flowforge.orchestrator.engine.CapacityProvider;
import com.flowforge.orchestrator.engine.DispatchJob;
import com.flowforge.orchestrator.engine.DispatchService;
import com.flowforge.orchestrator.engine.FailureHandler;
import com.flowforge.orchestrator.engine.LocalCapacityProvider;
import com.flowforge.orchestrator.engine.LocalResultSink;
import com.flowforge.orchestrator.engine.LocalTaskDispatcher;
import com.flowforge.orchestrator.engine.QueueReconciler;
import com.flowforge.orchestrator.engine.RecurringJob;
import com.flowforge.orchestrator.engine.ResultProcessor;
import com.flowforge.orchestrator.engine.RetryPromoter;
import com.flowforge.orchestrator.engine.TaskDispatcher;
import com.flowforge.orchestrator.engine.TimeoutSweeper;
import com.flowforge.orchestrator.kafka.KafkaCapacityProvider;
import com.flowforge.orchestrator.kafka.KafkaTaskDispatcher;
import com.flowforge.orchestrator.kafka.ResultConsumer;
import com.flowforge.orchestrator.kafka.ResultConsumers;
import com.flowforge.orchestrator.infrastructure.messaging.OutboxWriter;
import com.flowforge.orchestrator.infrastructure.persistence.DeadLetterJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.EngineJdbcRepository;
import com.flowforge.orchestrator.infrastructure.persistence.ExecutionJdbcRepository;
import com.flowforge.runtime.claim.TaskClaimRepository;
import com.flowforge.runtime.claim.TaskInputRepository;
import com.flowforge.runtime.executor.OutputGuard;
import com.flowforge.runtime.runner.TaskRunner;
import com.flowforge.runtime.runner.TaskRunnerSettings;
import com.flowforge.runtime.runner.ThreadMode;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.random.RandomGenerator;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.transaction.support.TransactionTemplate;

/** The orchestration loop: result processing, failures and retries, dispatch, background jobs, local runner. */
@Configuration
public class EngineConfig {

    @Bean
    ReadyTaskSignal readyTaskSignal() {
        return new ReadyTaskSignal();
    }

    @Bean
    FailureHandler failureHandler(EngineJdbcRepository engine, DeadLetterJdbcRepository deadLetters, ObjectMapper json,
                                  RandomGenerator random) {
        return new FailureHandler(engine, deadLetters, json, random);
    }

    @Bean
    EventPublisher eventPublisher(ExecutionJdbcRepository executions, OutboxWriter outbox, ObjectMapper json) {
        return new EventPublisher(executions, outbox, json);
    }

    @Bean
    ResultProcessor resultProcessor(EngineJdbcRepository engine, EventPublisher publisher, PublishedDagCache dags,
                                    FailureHandler failures, TransactionTemplate transaction, Clock clock,
                                    ObjectMapper json, ReadyTaskSignal readyTasks, MeterRegistry meters) {
        return new ResultProcessor(engine, publisher, dags, failures, transaction, clock, json, readyTasks, meters);
    }

    @Bean
    RetryPromoter retryPromoter(EngineJdbcRepository engine, EventPublisher publisher,
                                TransactionTemplate transaction, Clock clock, ReadyTaskSignal readyTasks, ObjectMapper json,
                                JobsProperties jobs) {
        return new RetryPromoter(engine, publisher, transaction, clock, readyTasks, json, jobs.batchSize());
    }

    @Bean
    TimeoutSweeper timeoutSweeper(EngineJdbcRepository engine, EventPublisher publisher,
                                  FailureHandler failures, TransactionTemplate transaction, Clock clock,
                                  ObjectMapper json, TimeoutProperties timeout, JobsProperties jobs) {
        return new TimeoutSweeper(engine, publisher, failures, transaction, clock, json, timeout.grace(),
            jobs.batchSize());
    }

    @Bean
    DeadLetterService deadLetterService(DeadLetterJdbcRepository deadLetters, EngineJdbcRepository engine,
                                        ExecutionJdbcRepository executions, EventPublisher publisher,
                                        TransactionTemplate transaction, Clock clock, ObjectMapper json,
                                        ReadyTaskSignal readyTasks) {
        return new DeadLetterService(deadLetters, engine, executions, publisher, transaction, clock, json, readyTasks);
    }

    @Bean
    WorkerHeartbeats workerHeartbeats(StringRedisTemplate redis) {
        return new WorkerHeartbeats(redis);
    }

    @Bean
    RecoveryService recoveryService(EngineJdbcRepository engine, EventPublisher publisher, FailureHandler failures,
                                    TransactionTemplate transaction, Clock clock, ObjectMapper json,
                                    ReadyTaskSignal readyTasks) {
        return new RecoveryService(engine, publisher, failures, transaction, clock, json, readyTasks);
    }

    @Bean
    WorkerMonitor workerMonitor(WorkerJdbcRepository workers, EngineJdbcRepository engine, WorkerHeartbeats heartbeats,
                                RecoveryService recovery, EventPublisher publisher, TransactionTemplate transaction,
                                Clock clock, ObjectMapper json, WorkersProperties properties, MeterRegistry meters) {
        return new WorkerMonitor(workers, engine, heartbeats, recovery, publisher, transaction, clock, json,
            properties.heartbeatTtl(), meters);
    }

    @Bean
    QueueDepthSampler queueDepthSampler(JdbcTemplate jdbc, MeterRegistry meters) {
        return new QueueDepthSampler(jdbc, meters);
    }

    @Bean
    WorkerService workerService(WorkerJdbcRepository workers, WorkerHeartbeats heartbeats, EventPublisher publisher,
                                TransactionTemplate transaction, Clock clock) {
        return new WorkerService(workers, heartbeats, publisher, transaction, clock);
    }

    /** The background engine jobs (both dispatch modes). */
    @Configuration
    @ConditionalOnProperty(name = "flowforge.jobs.enabled", havingValue = "true", matchIfMissing = true)
    static class Jobs {

        @Bean
        RecurringJob retryPromoterJob(RetryPromoter promoter, RetryProperties retry) {
            return new RecurringJob("retry-promoter", retry.promoterInterval(), promoter::promoteDue);
        }

        @Bean
        RecurringJob timeoutSweeperJob(TimeoutSweeper sweeper, TimeoutProperties timeout) {
            return new RecurringJob("timeout-sweeper", timeout.sweepInterval(), sweeper::sweep);
        }

        @Bean
        RecurringJob workerMonitorJob(WorkerMonitor monitor, WorkersProperties workers) {
            return new RecurringJob("worker-monitor", workers.monitorInterval(), monitor::monitorOnce);
        }

        @Bean
        RecurringJob queueDepthJob(QueueDepthSampler sampler, WorkersProperties workers) {
            return new RecurringJob("queue-depth", workers.queueDepthInterval(), sampler::sample);
        }
    }

    /** Local mode: tasks run inside the orchestrator ({@code flowforge.dispatch.mode=local}). */
    @Configuration
    @ConditionalOnProperty(name = "flowforge.dispatch.mode", havingValue = "local")
    static class LocalMode {

        @Bean
        TaskRunner localTaskRunner(TaskExecutorRegistry executors, DataSource dataSource, ObjectMapper json,
                                   ResultProcessor results, OutputGuard outputGuard, Clock clock,
                                   LocalRunnerProperties runner, RandomGenerator random) {
            String workerId = "local-" + HexFormat.of().toHexDigits(random.nextInt());
            return new TaskRunner(new TaskRunnerSettings(workerId, runner.maxConcurrency(), ThreadMode.PLATFORM,
                Duration.ofSeconds(2), 5, Duration.ofMillis(100)), executors, new TaskClaimRepository(dataSource),
                new TaskInputRepository(dataSource, json), new LocalResultSink(results), outputGuard, clock);
        }

        @Bean
        LocalTaskDispatcher localTaskDispatcher(TaskRunner runner) {
            return new LocalTaskDispatcher(runner);
        }

        @Bean
        CapacityProvider localCapacityProvider(EngineJdbcRepository engine, LocalRunnerProperties runner) {
            return new LocalCapacityProvider(engine, runner.maxConcurrency());
        }
    }

    /** Kafka mode: commands go to workers through the outbox. */
    @Configuration
    @ConditionalOnProperty(name = "flowforge.dispatch.mode", havingValue = "kafka", matchIfMissing = true)
    static class KafkaMode {

        @Bean
        KafkaTaskDispatcher kafkaTaskDispatcher(OutboxWriter outbox, ObjectMapper json, Clock clock) {
            return new KafkaTaskDispatcher(outbox, json, clock);
        }

        @Bean
        CapacityProvider kafkaCapacityProvider(EngineJdbcRepository engine, WorkerJdbcRepository workers) {
            return new KafkaCapacityProvider(engine, workers);
        }

        @Bean
        ResultConsumers resultConsumers(ConcurrentKafkaListenerContainerFactory<?, ?> factory, ResultProcessor results,
                                        ObjectMapper json) {
            return new ResultConsumers(factory, new ResultConsumer(results, json), 3);
        }
    }

    // ---- dispatch, the same in both modes (the mode supplies TaskDispatcher and CapacityProvider) ----

    @Bean
    DispatchService dispatchService(EngineJdbcRepository engine, EventPublisher publisher,
                                    TaskDispatcher dispatcher, CapacityProvider capacity,
                                    TransactionTemplate transaction, Clock clock, ObjectMapper json,
                                    DispatchProperties properties) {
        return new DispatchService(engine, publisher, dispatcher, capacity, transaction, clock, json,
            properties.batchSize());
    }

    @Bean
    QueueReconciler queueReconciler(EngineJdbcRepository engine, DispatchService dispatch, Clock clock,
                                    ReconcilerProperties reconciler, JobsProperties jobs, MeterRegistry meters) {
        return new QueueReconciler(engine, dispatch, clock, reconciler.staleQueuedAfter(), jobs.batchSize(), meters);
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.jobs.enabled", havingValue = "true", matchIfMissing = true)
    RecurringJob queueReconcilerJob(QueueReconciler reconciler, ReconcilerProperties properties) {
        return new RecurringJob("queue-reconciler", properties.interval(), reconciler::reconcile);
    }

    @Bean
    @ConditionalOnProperty(name = "flowforge.dispatch.job-enabled", havingValue = "true", matchIfMissing = true)
    DispatchJob dispatchJob(DispatchService dispatch, ReadyTaskSignal readyTasks, DispatchProperties properties) {
        return new DispatchJob(dispatch, readyTasks, properties.pollInterval());
    }
}

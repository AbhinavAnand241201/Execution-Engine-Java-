package com.flowforge.worker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.Topics;
import com.flowforge.core.executor.DelayTaskExecutor;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskExecutorRegistry;
import com.flowforge.runtime.claim.TaskClaimRepository;
import com.flowforge.runtime.claim.TaskInputRepository;
import com.flowforge.runtime.executor.DatabaseExecutorSettings;
import com.flowforge.runtime.executor.DatabaseTaskExecutor;
import com.flowforge.runtime.executor.HttpExecutorSettings;
import com.flowforge.runtime.executor.HttpTaskExecutor;
import com.flowforge.runtime.executor.OutputGuard;
import com.flowforge.runtime.executor.ReportTaskExecutor;
import com.flowforge.runtime.runner.TaskRunner;
import com.flowforge.runtime.runner.TaskRunnerSettings;
import com.flowforge.worker.messaging.CommandConsumers;
import com.flowforge.worker.messaging.CommandListener;
import com.flowforge.worker.messaging.InvalidMessageException;
import com.flowforge.worker.lifecycle.WorkerLifecycle;
import com.flowforge.worker.messaging.KafkaResultSink;
import com.flowforge.worker.registry.HeartbeatService;
import com.flowforge.worker.registry.WorkerFreeze;
import com.flowforge.worker.registry.WorkerRegistration;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;
import javax.sql.DataSource;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.support.TransactionTemplate;
import com.flowforge.runtime.db.DatabaseErrors;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

/** A worker: executors, task runner, Kafka consumers and result sink, registry, heartbeat and lifecycle. */
@Configuration
public class WorkerConfig {

    /** Exit status when this incarnation finds itself declared OFFLINE (EX_SOFTWARE); docker restarts it. */
    static final int EXIT_DECLARED_OFFLINE = 70;

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    /** Injectable randomness. ThreadLocalRandom's shared instance is safe to call from any thread. */
    @Bean
    RandomGenerator randomGenerator() {
        return ThreadLocalRandom.current();
    }

    @Bean
    WorkerIdentity workerIdentity(WorkerProperties worker, RandomGenerator random) {
        return new WorkerIdentity(worker.name() + "-" + HexFormat.of().toHexDigits(random.nextInt()), worker.name());
    }

    @Bean
    DelayTaskExecutor delayTaskExecutor() {
        return new DelayTaskExecutor();
    }

    @Bean
    HttpTaskExecutor httpTaskExecutor(ExecutorsProperties executors, ObjectMapper json, Clock clock) {
        ExecutorsProperties.Http http = executors.http();
        return new HttpTaskExecutor(new HttpExecutorSettings(Set.copyOf(http.allowedHosts()), http.connectTimeout(),
            http.maxResponseBytes()), json, clock);
    }

    @Bean
    DatabaseTaskExecutor databaseTaskExecutor(DataSource dataSource, ExecutorsProperties executors, ObjectMapper json) {
        ExecutorsProperties.Database database = executors.database();
        return new DatabaseTaskExecutor(dataSource, new DatabaseExecutorSettings(database.queries(),
            database.statementTimeout(), database.maxRows()), json);
    }

    @Bean
    ReportTaskExecutor reportTaskExecutor(ObjectMapper json) {
        return new ReportTaskExecutor(json);
    }

    @Bean
    TaskExecutorRegistry taskExecutorRegistry(List<TaskExecutor> executors) {
        return new TaskExecutorRegistry(executors);
    }

    @Bean
    WorkerFreeze workerFreeze() {
        return new WorkerFreeze();
    }

    @Bean
    TaskRunner taskRunner(WorkerIdentity identity, WorkerProperties worker, TaskExecutorRegistry executors,
                          DataSource dataSource, ObjectMapper json, KafkaTemplate<String, String> kafka,
                          LimitsProperties limits, WorkerFreeze freeze, Clock clock) {
        return new TaskRunner(new TaskRunnerSettings(identity.id(), worker.maxConcurrency(), worker.threadMode(),
            Duration.ofSeconds(2), 5, Duration.ofMillis(200)), executors, new TaskClaimRepository(dataSource),
            new TaskInputRepository(dataSource, json), new KafkaResultSink(kafka, json, Duration.ofSeconds(10), freeze),
            new OutputGuard(limits.outputMaxBytes()), clock);
    }

    /**
     * Failed records: retried 3 times, 1 s apart, then published to {@code <topic>.DLT} (same partition). An
     * unreadable record skips the retries.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafka,
            (record, e) -> new TopicPartition(record.topic() + Topics.DEAD_LETTER_SUFFIX, record.partition()));
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3));
        handler.addNotRetryableExceptions(InvalidMessageException.class);
        // The database being unreachable says nothing about the record: retry it (backing off up to 10 s) until the
        // database is back, never dead-letter it. Otherwise a short outage would throw away valid results/commands.
        handler.setBackOffFunction((record, e) -> databaseUnavailable(e) ? untilDatabaseIsBack() : null);
        return handler;
    }

    @Bean
    CommandConsumers commandConsumers(ConcurrentKafkaListenerContainerFactory<?, ?> factory, TaskRunner runner,
                                      ObjectMapper json, WorkerProperties worker) {
        return new CommandConsumers(factory, new CommandListener(runner, json), worker.capabilities(),
            worker.listenerConcurrency());
    }

    @Bean
    WorkerRegistration workerRegistration(JdbcTemplate jdbc, TransactionTemplate transaction, ObjectMapper json) {
        return new WorkerRegistration(jdbc, transaction, json);
    }

    /** The drain flag (set by an operator through the orchestrator) makes the worker stop consuming. */
    @Bean
    HeartbeatService heartbeatService(StringRedisTemplate redis, ObjectMapper json, TaskRunner runner,
                                      WorkerFreeze freeze, Clock clock, WorkerIdentity identity,
                                      WorkerProperties worker, CommandConsumers consumers,
                                      WorkerRegistration registration) {
        return new HeartbeatService(redis, json, runner, freeze, clock, identity.id(), worker.capabilities(),
            worker.maxConcurrency(), worker.heartbeatInterval(), worker.heartbeatTtl(), drain -> {
                if (drain && !consumers.isDraining()) {
                    // Stopping containers can take seconds: never on the heartbeat thread, or beats would be missed.
                    Thread.ofVirtual().name("ff-drain").start(consumers::drain);
                }
            }, () -> registration.status(identity.id()), () -> {
                consumers.drain();
                if (worker.exitWhenDeclaredOffline()) {
                    Thread.ofPlatform().name("ff-exit").start(() -> System.exit(EXIT_DECLARED_OFFLINE));
                }
            });
    }

    @Bean
    WorkerLifecycle workerLifecycle(WorkerRegistration registration, HeartbeatService heartbeat,
                                    CommandConsumers consumers, TaskRunner runner, WorkerFreeze freeze, Clock clock,
                                    WorkerIdentity identity, WorkerProperties worker) {
        return new WorkerLifecycle(registration, heartbeat, consumers, runner, freeze, clock,
            () -> new WorkerLifecycle.Registration(identity.id(), identity.name(), hostname(), worker.maxConcurrency()),
            worker.capabilities(), worker.shutdownGrace());
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    /** Spring's own "database unreachable" exceptions, or a JDBC connectivity error anywhere in the chain. */
    private static boolean databaseUnavailable(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof DataAccessResourceFailureException || cause instanceof TransientDataAccessException) {
                return true;
            }
        }
        return DatabaseErrors.isUnavailable(e);
    }

    private static ExponentialBackOff untilDatabaseIsBack() {
        ExponentialBackOff backOff = new ExponentialBackOff(500L, 2.0);
        backOff.setMaxInterval(10_000L);   // max elapsed time stays unlimited
        return backOff;
    }
}

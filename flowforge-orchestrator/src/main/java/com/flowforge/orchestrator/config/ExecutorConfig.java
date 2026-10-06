package com.flowforge.orchestrator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.runtime.executor.DatabaseExecutorSettings;
import com.flowforge.runtime.executor.DatabaseTaskExecutor;
import com.flowforge.runtime.executor.HttpExecutorSettings;
import com.flowforge.runtime.executor.HttpTaskExecutor;
import com.flowforge.runtime.executor.OutputGuard;
import com.flowforge.runtime.executor.ReportTaskExecutor;
import java.time.Clock;
import java.util.Set;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The runtime's task executors as beans. Each joins the {@code TaskExecutorRegistry} (see {@link WorkflowConfig}), so
 * workflow validation checks HTTP, DATABASE and REPORT configs too. DATABASE queries run on the orchestrator's own
 * datasource, against the {@code demo} schema (V100), through read-only transactions.
 */
@Configuration
public class ExecutorConfig {

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
    OutputGuard outputGuard(LimitsProperties limits) {
        return new OutputGuard(limits.outputMaxBytes());
    }
}

package com.flowforge.worker.config;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** {@code flowforge.executors.*}: the same executor settings the orchestrator validates against. */
@ConfigurationProperties("flowforge.executors")
public record ExecutorsProperties(Http http, Database database) {

    /** {@code allowedHosts} is the SSRF allowlist (exact host names); env {@code FLOWFORGE_HTTP_ALLOWED_HOSTS}. */
    public record Http(List<String> allowedHosts, Duration connectTimeout, int maxResponseBytes) {
    }

    /** The only SQL DATABASE tasks may run, by name, plus per-query limits. */
    public record Database(Map<String, String> queries, Duration statementTimeout, int maxRows) {
    }
}

package com.flowforge.runtime.executor;

import java.time.Duration;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code flowforge.executors.http.*}.
 *
 * @param allowedHosts     exact host names HTTP tasks may call (SSRF allowlist), compared case-insensitively
 * @param connectTimeout   TCP connect limit
 * @param maxResponseBytes response bodies are cut at this size
 */
public record HttpExecutorSettings(Set<String> allowedHosts, Duration connectTimeout, int maxResponseBytes) {

    public HttpExecutorSettings {
        if (maxResponseBytes < 1 || connectTimeout.isNegative() || connectTimeout.isZero()) {
            throw new IllegalArgumentException("max-response-bytes and connect-timeout must be positive");
        }
        allowedHosts = allowedHosts.stream().map(String::trim).filter(h -> !h.isEmpty())
            .map(h -> h.toLowerCase(Locale.ROOT)).collect(Collectors.toUnmodifiableSet());
    }

    /**
     * Exact, case-insensitive host match; IPv6 literals match without brackets ({@code [::1]} → {@code ::1}). Any port
     * on an allowed host is reachable, so allow only hosts whose every port is fine to call.
     */
    public boolean allows(String host) {
        if (host == null) {
            return false;
        }
        String bare = host.startsWith("[") && host.endsWith("]") ? host.substring(1, host.length() - 1) : host;
        return allowedHosts.contains(bare.toLowerCase(Locale.ROOT));
    }
}

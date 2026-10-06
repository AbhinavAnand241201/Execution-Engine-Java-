package com.flowforge.runtime.executor;

import static com.flowforge.runtime.executor.ConfigChecks.invalid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Calls an HTTP endpoint. Three protections matter more than the call itself:
 * <ul>
 *   <li><b>SSRF:</b> the URL's host must be on an exact-match allowlist, checked when the workflow is validated and
 *   again right before the call (config could have been stored before the allowlist changed). Redirects are never
 *   followed, so an allowed host can't bounce the request somewhere else.</li>
 *   <li><b>Safe retries:</b> every attempt sends {@code Idempotency-Key: <taskExecutionId>}, the same on every retry, so
 *   the receiving service can ignore a repeat of a request it already handled.</li>
 *   <li><b>Bounded memory and time:</b> the body is read up to {@code maxResponseBytes} (the rest is discarded) and the
 *   whole exchange, body included, must finish before the task's timeout.</li>
 * </ul>
 * Known limitation: the allowlist checks the host name, not the IP it resolves to, so DNS rebinding (an allowed name
 * that later resolves to an internal address) is not stopped; resolving once and rejecting private/loopback ranges
 * would close it.
 */
public final class HttpTaskExecutor implements TaskExecutor, AutoCloseable {

    static final Set<String> METHODS = Set.of("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD");
    static final long MAX_TIMEOUT_MS = 300_000;
    static final long DEFAULT_TIMEOUT_MS = 30_000;
    /** The JDK refuses to let callers set these; reject them at validation instead of failing at run time. */
    private static final Set<String> RESTRICTED_HEADERS = Set.of("connection", "content-length", "expect", "host",
        "upgrade", "idempotency-key", "x-correlation-id");
    private static final Set<String> FIELDS = Set.of("method", "url", "headers", "body", "expectedStatus", "timeoutMs");

    private final HttpExecutorSettings settings;
    private final HttpClient client;
    private final ObjectMapper json;
    private final Clock clock;

    public HttpTaskExecutor(HttpExecutorSettings settings, ObjectMapper json, Clock clock) {
        this.settings = settings;
        this.json = json;
        this.clock = clock;
        this.client = HttpClient.newBuilder()
            .connectTimeout(settings.connectTimeout())
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();
    }

    @Override
    public TaskType supportedType() {
        return TaskType.HTTP;
    }

    /** Stops the client's selector thread and open connections (Spring calls this on shutdown). */
    @Override
    public void close() {
        client.close();
    }

    @Override
    public List<ValidationError> validateConfig(JsonNode config) {
        List<ValidationError> errors = new ArrayList<>();
        String method = ConfigChecks.requiredText(config, "method", errors);
        if (method != null && !METHODS.contains(method)) {
            errors.add(invalid("method", "method must be one of " + new TreeSet<>(METHODS)));
        }
        String url = ConfigChecks.requiredText(config, "url", errors);
        if (url != null) {
            checkUrl(url, errors);
        }
        JsonNode headers = config.get("headers");
        if (headers != null) {
            if (!headers.isObject()) {
                errors.add(invalid("headers", "headers must be an object of strings"));
            } else {
                headers.fields().forEachRemaining(h -> {
                    if (!h.getValue().isTextual()) {
                        errors.add(invalid("headers." + h.getKey(), "header values must be strings"));
                    } else if (RESTRICTED_HEADERS.contains(h.getKey().toLowerCase(Locale.ROOT))) {
                        errors.add(invalid("headers." + h.getKey(), "header '" + h.getKey() + "' is set by FlowForge"));
                    } else if (!jdkAccepts(h.getKey(), h.getValue().asText())) {
                        errors.add(invalid("headers." + h.getKey(), "invalid header name or value"));
                    }
                });
            }
        }
        JsonNode expected = config.get("expectedStatus");
        if (expected != null && !validRange(expected)) {
            errors.add(invalid("expectedStatus", "expectedStatus must be [min, max] with 100 <= min <= max <= 599"));
        }
        JsonNode timeout = config.get("timeoutMs");
        if (timeout != null && (!timeout.isIntegralNumber() || timeout.asLong() < 1 || timeout.asLong() > MAX_TIMEOUT_MS)) {
            errors.add(invalid("timeoutMs", "timeoutMs must be a whole number between 1 and " + MAX_TIMEOUT_MS));
        }
        ConfigChecks.rejectUnknownFields(config, FIELDS, errors);
        return errors;
    }

    private void checkUrl(String url, List<ValidationError> errors) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (Exception e) {
            errors.add(invalid("url", "url is not a valid URI"));
            return;
        }
        if (!"http".equalsIgnoreCase(uri.getScheme()) && !"https".equalsIgnoreCase(uri.getScheme())) {
            errors.add(invalid("url", "url must be an absolute http or https URL"));
        } else if (uri.getUserInfo() != null) {
            errors.add(invalid("url", "url must not contain credentials"));
        } else if (!settings.allows(uri.getHost())) {
            errors.add(invalid("url", "host '" + uri.getHost() + "' is not in the HTTP allowlist"));
        } else if (uri.getPort() > 65_535) {
            errors.add(invalid("url", "port must be between 1 and 65535"));
        } else {
            try {
                HttpRequest.newBuilder(uri);
            } catch (IllegalArgumentException e) {
                errors.add(invalid("url", "url is not usable for HTTP"));
            }
        }
    }

    /** The JDK's own rules (no CR/LF, valid token name), checked now rather than failing every attempt later. */
    private static boolean jdkAccepts(String name, String value) {
        try {
            HttpRequest.newBuilder().header(name, value);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private static boolean validRange(JsonNode range) {
        return range.isArray() && range.size() == 2 && range.get(0).isIntegralNumber() && range.get(1).isIntegralNumber()
            && range.get(0).asInt() >= 100 && range.get(0).asInt() <= range.get(1).asInt() && range.get(1).asInt() <= 599;
    }

    @Override
    public TaskResult execute(TaskContext context) throws InterruptedException {
        JsonNode config = context.config();
        List<ValidationError> problems = validateConfig(config);
        if (!problems.isEmpty()) {
            // Covers the allowlist changing after the workflow was published.
            return TaskResult.failure("INVALID_TASK_CONFIG", problems.getFirst().message(), false);
        }
        Duration timeout = timeout(context);
        if (timeout.isZero()) {
            return TaskResult.failure("TIMEOUT", "no time left before the task deadline", true);
        }
        HttpRequest request = buildRequest(context, config, timeout);

        // Completed by whichever comes first: the client's response, or the body reader once it has cut the body
        // (cancelling the download afterwards may make the client's own future fail; by then it no longer matters).
        CompletableFuture<Exchange> exchange = new CompletableFuture<>();
        CompletableFuture<HttpResponse<LimitedBody>> future = client.sendAsync(request,
            info -> new LimitedBodySubscriber(settings.maxResponseBytes(), info, exchange));
        future.whenComplete((response, error) -> {
            if (error != null) {
                exchange.completeExceptionally(error instanceof CompletionException ? error.getCause() : error);
            } else {
                exchange.complete(new Exchange(response.statusCode(), response.headers(), response.body()));
            }
        });
        context.cancellation().onCancel(() -> future.cancel(true));
        Exchange response;
        try {
            response = exchange.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            return TaskResult.failure("TIMEOUT", "no complete response within " + timeout.toMillis() + " ms", true);
        } catch (java.util.concurrent.CancellationException e) {
            return TaskResult.cancelled();
        } catch (InterruptedException e) {
            future.cancel(true);
            throw e;
        } catch (ExecutionException e) {
            if (context.cancellation().isCancelled()) {
                return TaskResult.cancelled();
            }
            Throwable cause = e.getCause() instanceof CompletionException wrapped ? wrapped.getCause() : e.getCause();
            if (cause instanceof java.util.concurrent.CancellationException) {
                return TaskResult.cancelled();
            }
            if (cause instanceof HttpTimeoutException) {
                return TaskResult.failure("TIMEOUT", "HTTP " + cause.getClass().getSimpleName(), true);
            }
            if (cause instanceof IOException) {
                return TaskResult.failure("HTTP_IO_ERROR", "I/O error: " + cause.getClass().getSimpleName(), true);
            }
            return TaskResult.failure("HTTP_ERROR", "request failed: " + cause.getClass().getSimpleName(), false);
        }
        return classify(config, response);
    }

    private Duration timeout(TaskContext context) {
        long configured = context.config().path("timeoutMs").asLong(DEFAULT_TIMEOUT_MS);
        if (context.deadline() == null) {
            return Duration.ofMillis(configured);
        }
        long untilDeadline = Duration.between(clock.instant(), context.deadline()).toMillis();
        return Duration.ofMillis(Math.max(0, Math.min(configured, untilDeadline)));
    }

    private HttpRequest buildRequest(TaskContext context, JsonNode config, Duration timeout) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(config.get("url").asText())).timeout(timeout);
        boolean contentTypeSet = false;
        JsonNode headers = config.path("headers");
        for (var it = headers.fields(); it.hasNext(); ) {
            var header = it.next();
            builder.header(header.getKey(), header.getValue().asText());
            contentTypeSet |= header.getKey().equalsIgnoreCase("content-type");
        }
        builder.header("Idempotency-Key", context.taskExecutionId().toString());
        if (context.correlationId() != null) {
            builder.header("X-Correlation-Id", context.correlationId());
        }
        JsonNode body = config.get("body");
        HttpRequest.BodyPublisher publisher = HttpRequest.BodyPublishers.noBody();
        if (body != null && !body.isNull()) {
            publisher = HttpRequest.BodyPublishers.ofString(body.isTextual() ? body.asText() : body.toString());
            if (!contentTypeSet) {
                builder.header("Content-Type", body.isTextual() ? "text/plain; charset=utf-8" : "application/json");
            }
        }
        return builder.method(config.get("method").asText(), publisher).build();
    }

    private TaskResult classify(JsonNode config, Exchange response) {
        int status = response.statusCode();
        JsonNode range = config.get("expectedStatus");
        int min = range == null ? 200 : range.get(0).asInt();
        int max = range == null ? 299 : range.get(1).asInt();
        if (status >= min && status <= max) {
            return TaskResult.success(output(response));
        }
        boolean retryable = status == 408 || status == 429 || status >= 500;
        return TaskResult.failure("HTTP_STATUS", "HTTP " + status + " is outside the expected range [" + min + ", "
            + max + "]", retryable);
    }

    private ObjectNode output(Exchange response) {
        ObjectNode output = json.createObjectNode();
        output.put("status", response.statusCode());
        ObjectNode headers = output.putObject("headers");
        response.headers().firstValue("content-type").ifPresent(type -> headers.put("content-type", type));
        LimitedBody body = response.body();
        String text = new String(body.bytes(), charset(response));
        JsonNode parsed = null;
        if (!body.truncated() && !text.isBlank()) {
            try {
                parsed = json.readTree(text);
            } catch (IOException notJson) {
                parsed = null;
            }
        }
        if (parsed != null) {
            output.set("body", parsed);
        } else {
            output.put("body", text);
        }
        if (body.truncated()) {
            output.put("truncated", true);
        }
        return output;
    }

    /** The charset named in Content-Type, else UTF-8 (JSON's default). */
    private static java.nio.charset.Charset charset(Exchange response) {
        return response.headers().firstValue("content-type")
            .map(type -> {
                for (String part : type.split(";")) {
                    String trimmed = part.trim();
                    if (trimmed.regionMatches(true, 0, "charset=", 0, 8)) {
                        try {
                            return java.nio.charset.Charset.forName(trimmed.substring(8).replace("\"", ""));
                        } catch (IllegalArgumentException unknown) {
                            return StandardCharsets.UTF_8;
                        }
                    }
                }
                return StandardCharsets.UTF_8;
            })
            .orElse(StandardCharsets.UTF_8);
    }

    /** Status, headers and (possibly cut) body of one HTTP exchange. */
    record Exchange(int statusCode, HttpHeaders headers, LimitedBody body) {
    }

    /** The first {@code limit} bytes of a body and whether anything was cut. */
    record LimitedBody(byte[] bytes, boolean truncated) {
    }

    /**
     * Keeps the first {@code limit} bytes, then cancels the download, so a huge or endless response costs neither
     * memory nor time: the task gets a truncated success instead of a timeout. Requests one chunk at a time.
     */
    static final class LimitedBodySubscriber implements HttpResponse.BodySubscriber<LimitedBody> {

        private final int limit;
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final CompletableFuture<LimitedBody> result = new CompletableFuture<>();
        private final HttpResponse.ResponseInfo info;
        private final CompletableFuture<Exchange> exchange;
        private Flow.Subscription subscription;

        LimitedBodySubscriber(int limit, HttpResponse.ResponseInfo info, CompletableFuture<Exchange> exchange) {
            this.limit = limit;
            this.info = info;
            this.exchange = exchange;
        }

        @Override
        public CompletionStage<LimitedBody> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            for (ByteBuffer item : items) {
                int take = Math.min(limit - buffer.size(), item.remaining());
                if (take > 0) {
                    byte[] chunk = new byte[take];
                    item.get(chunk);
                    buffer.write(chunk, 0, take);
                }
                if (item.hasRemaining()) {
                    LimitedBody cut = new LimitedBody(buffer.toByteArray(), true);
                    exchange.complete(new Exchange(info.statusCode(), info.headers(), cut));
                    result.complete(cut);
                    subscription.cancel();
                    return;
                }
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable error) {
            result.completeExceptionally(error);
        }

        @Override
        public void onComplete() {
            result.complete(new LimitedBody(buffer.toByteArray(), false));
        }
    }
}

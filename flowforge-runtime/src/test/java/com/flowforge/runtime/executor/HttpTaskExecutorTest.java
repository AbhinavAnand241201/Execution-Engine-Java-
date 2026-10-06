package com.flowforge.runtime.executor;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static com.flowforge.runtime.executor.Contexts.context;
import static com.flowforge.runtime.executor.Contexts.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.executor.CancellationToken;
import com.flowforge.core.executor.TaskResult;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.net.ServerSocket;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class HttpTaskExecutorTest {

    private static WireMockServer wiremock;
    private static HttpTaskExecutor executor;

    @BeforeAll
    static void start() {
        wiremock = new WireMockServer(options().dynamicPort());
        wiremock.start();
        executor = new HttpTaskExecutor(new HttpExecutorSettings(Set.of("localhost", "::1"), Duration.ofMillis(500),
            16_384), Contexts.JSON, Clock.systemUTC());
    }

    @AfterAll
    static void stop() {
        executor.close();
        wiremock.stop();
    }

    @BeforeEach
    void reset() {
        wiremock.resetAll();
    }

    private String url(String path) {
        return "http://localhost:" + wiremock.port() + path;
    }

    private JsonNode getConfig(String path) {
        return Contexts.JSON.createObjectNode().put("method", "GET").put("url", url(path));
    }

    private TaskResult run(JsonNode config) throws InterruptedException {
        return executor.execute(context(config));
    }

    @Test
    @DisplayName("T10.1 HTTP 200 with a JSON body → Success with the parsed body, status and content type")
    void success() throws Exception {
        wiremock.stubFor(get("/orders/1").willReturn(aResponse().withStatus(200)
            .withHeader("Content-Type", "application/json").withBody("{\"id\":1,\"total\":9.5}")));

        TaskResult result = run(getConfig("/orders/1"));

        assertThat(result).isInstanceOf(TaskResult.Success.class);
        JsonNode output = ((TaskResult.Success) result).output();
        assertThat(output.get("status").asInt()).isEqualTo(200);
        assertThat(output.get("headers").get("content-type").asText()).isEqualTo("application/json");
        assertThat(output.get("body").get("total").asDouble()).isEqualTo(9.5);
    }

    @ParameterizedTest(name = "HTTP {0}")
    @ValueSource(ints = {500, 503, 429, 408})
    @DisplayName("T10.2 500, 503, 429 and 408 → retryable failure")
    void retryableStatuses(int status) throws Exception {
        wiremock.stubFor(get("/s").willReturn(aResponse().withStatus(status)));
        assertThat(run(getConfig("/s"))).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.retryable()).isTrue();
            assertThat(f.message()).contains(String.valueOf(status));
        });
    }

    @ParameterizedTest(name = "HTTP {0}")
    @ValueSource(ints = {400, 404, 401, 422})
    @DisplayName("T10.2 400, 404, 401 and 422 → non-retryable failure")
    void permanentStatuses(int status) throws Exception {
        wiremock.stubFor(get("/s").willReturn(aResponse().withStatus(status)));
        assertThat(run(getConfig("/s"))).isInstanceOfSatisfying(TaskResult.Failure.class,
            f -> assertThat(f.retryable()).isFalse());
    }

    @Test
    @DisplayName("T10.2 expectedStatus widens success: 404 inside [200, 404] is a success")
    void expectedStatusRange() throws Exception {
        wiremock.stubFor(get("/s").willReturn(aResponse().withStatus(404).withBody("gone")));
        JsonNode config = ((com.fasterxml.jackson.databind.node.ObjectNode) getConfig("/s"));
        ((com.fasterxml.jackson.databind.node.ObjectNode) config).set("expectedStatus", json("[200, 404]"));
        assertThat(run(config)).isInstanceOfSatisfying(TaskResult.Success.class,
            s -> assertThat(s.output().get("body").asText()).isEqualTo("gone"));
    }

    @Test
    @DisplayName("T10.3 read timeout (slow response) → retryable TIMEOUT")
    void readTimeout() throws Exception {
        wiremock.stubFor(get("/slow").willReturn(aResponse().withStatus(200).withFixedDelay(2_000)));
        JsonNode config = Contexts.JSON.createObjectNode().put("method", "GET").put("url", url("/slow")).put("timeoutMs", 300);

        long started = System.nanoTime();
        TaskResult result = run(config);

        assertThat(result).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.errorCode()).isEqualTo("TIMEOUT");
            assertThat(f.retryable()).isTrue();
        });
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(1_500));
    }

    @Test
    @DisplayName("T10.3 connect timeout (listener with a full backlog) → retryable TIMEOUT; refused connection → retryable")
    void connectTimeout() throws Exception {
        // A listener that never accepts, with its tiny backlog filled: further SYNs are dropped, so only the connect
        // timeout can end the attempt. Works offline (no reliance on an unroutable address).
        try (ServerSocket listener = new ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress())) {
            List<java.net.Socket> fillers = new java.util.ArrayList<>();
            try (HttpTaskExecutor shortConnect = new HttpTaskExecutor(
                new HttpExecutorSettings(Set.of("127.0.0.1"), Duration.ofMillis(300), 16_384), Contexts.JSON,
                Clock.systemUTC())) {
                for (int i = 0; i < 8; i++) {
                    java.net.Socket filler = new java.net.Socket();
                    try {
                        filler.connect(listener.getLocalSocketAddress(), 200);
                        fillers.add(filler);
                    } catch (java.io.IOException backlogFull) {
                        filler.close();
                        break;
                    }
                }
                JsonNode config = Contexts.JSON.createObjectNode().put("method", "GET")
                    .put("url", "http://127.0.0.1:" + listener.getLocalPort() + "/x").put("timeoutMs", 5_000);
                long started = System.nanoTime();
                assertThat(shortConnect.execute(context(config))).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
                    assertThat(f.errorCode()).isEqualTo("TIMEOUT");
                    assertThat(f.retryable()).isTrue();
                });
                assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(3));
            } finally {
                for (java.net.Socket filler : fillers) {
                    filler.close();
                }
            }
        }

        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        JsonNode refused = Contexts.JSON.createObjectNode().put("method", "GET").put("url",
            "http://localhost:" + closedPort + "/x");
        assertThat(run(refused)).isInstanceOfSatisfying(TaskResult.Failure.class, f -> assertThat(f.retryable()).isTrue());

        wiremock.stubFor(get("/reset").willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER)));
        assertThat(run(getConfig("/reset"))).isInstanceOfSatisfying(TaskResult.Failure.class,
            f -> assertThat(f.retryable()).isTrue());
    }

    @Test
    @DisplayName("T10.4 a host outside the allowlist is rejected at validation and again at execution; redirects are "
        + "not followed")
    void ssrf() throws Exception {
        JsonNode evil = Contexts.JSON.createObjectNode().put("method", "GET").put("url", "http://169.254.169.254/latest");
        assertThat(executor.validateConfig(evil)).singleElement()
            .satisfies(e -> assertThat(e.path()).isEqualTo("url"));
        assertThat(run(evil)).isInstanceOfSatisfying(TaskResult.Failure.class, f -> {
            assertThat(f.errorCode()).isEqualTo("INVALID_TASK_CONFIG");
            assertThat(f.retryable()).isFalse();
        });

        wiremock.stubFor(get("/redirect").willReturn(aResponse().withStatus(302)
            .withHeader("Location", "http://169.254.169.254/latest")));
        assertThat(run(getConfig("/redirect"))).isInstanceOfSatisfying(TaskResult.Failure.class,
            f -> assertThat(f.message()).contains("302"));
        wiremock.verify(1, getRequestedFor(urlEqualTo("/redirect")));

        assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"http://user:pw@localhost/x\"}")))
            .isNotEmpty();
        assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"file:///etc/passwd\"}"))).isNotEmpty();
        assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"http://LOCALHOST:1/x\"}"))).isEmpty();
        assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"http://[::1]:1/x\"}"))).isEmpty();
        assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"http://localhost:99999/x\"}")))
            .isNotEmpty();
    }

    @Test
    @DisplayName("T10.extra-5 header names/values the JDK would refuse (CR/LF injection, spaces, empty) fail validation")
    void badHeaders() {
        for (String headers : List.of("{\"X-A\":\"a\\r\\nInjected: b\"}", "{\"bad name\":\"x\"}", "{\"\":\"x\"}")) {
            assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"" + url("/x") + "\",\"headers\":"
                + headers + "}"))).as(headers).isNotEmpty();
        }
    }

    @Test
    @DisplayName("T10.5 Idempotency-Key = taskExecutionId on every attempt; X-Correlation-Id is sent; JSON body posted")
    void idempotencyKey() throws Exception {
        wiremock.stubFor(post("/charge").willReturn(aResponse().withStatus(503)));
        UUID taskExecutionId = UUID.randomUUID();
        JsonNode config = json("{\"method\":\"POST\",\"url\":\"" + url("/charge") + "\",\"body\":{\"amount\":5},"
            + "\"headers\":{\"X-Tenant\":\"t1\"}}");

        for (int attempt = 1; attempt <= 3; attempt++) {
            executor.execute(context(taskExecutionId, attempt, config, Map.of(), null, new CancellationToken()));
        }

        List<LoggedRequest> requests = wiremock.findAll(postRequestedFor(urlEqualTo("/charge")));
        assertThat(requests).hasSize(3).allSatisfy(r -> {
            assertThat(r.getHeader("Idempotency-Key")).isEqualTo(taskExecutionId.toString());
            assertThat(r.getHeader("X-Correlation-Id")).isEqualTo("corr-" + taskExecutionId);
            assertThat(r.getHeader("X-Tenant")).isEqualTo("t1");
            assertThat(r.getHeader("Content-Type")).isEqualTo("application/json");
            assertThat(r.getBodyAsString()).isEqualTo("{\"amount\":5}");
        });
        wiremock.verify(3, postRequestedFor(urlEqualTo("/charge")).withHeader("Idempotency-Key",
            equalTo(taskExecutionId.toString())));
        assertThat(executor.validateConfig(json("{\"method\":\"GET\",\"url\":\"" + url("/x")
            + "\",\"headers\":{\"Idempotency-Key\":\"mine\"}}"))).isNotEmpty();
    }

    @Test
    @DisplayName("T10.6 a response body over max-response-bytes (16 KB) is cut and marked truncated")
    void truncation() throws Exception {
        wiremock.stubFor(get("/big").willReturn(aResponse().withStatus(200).withBody("y".repeat(100_000))));

        TaskResult result = run(getConfig("/big"));

        assertThat(result).isInstanceOfSatisfying(TaskResult.Success.class, s -> {
            assertThat(s.output().get("body").asText()).hasSize(16_384);
            assertThat(s.output().get("truncated").asBoolean()).isTrue();
        });
    }

    @Test
    @DisplayName("T10.6 a body far larger than the limit (5 MB) ends as a truncated success quickly, not a timeout; "
        + "the Content-Type charset is honoured")
    void hugeBodyAndCharset() throws Exception {
        wiremock.stubFor(get("/huge").willReturn(aResponse().withStatus(200).withBody("q".repeat(5_000_000))
            .withChunkedDribbleDelay(500, 3_000)));
        JsonNode config = Contexts.JSON.createObjectNode().put("method", "GET").put("url", url("/huge"))
            .put("timeoutMs", 2_000);
        assertThat(run(config)).isInstanceOfSatisfying(TaskResult.Success.class,
            s -> assertThat(s.output().get("truncated").asBoolean()).isTrue());

        wiremock.stubFor(get("/latin").willReturn(aResponse().withStatus(200)
            .withHeader("Content-Type", "text/plain; charset=ISO-8859-1").withBody(new byte[] {(byte) 0xE9})));
        assertThat(run(getConfig("/latin"))).isInstanceOfSatisfying(TaskResult.Success.class,
            s -> assertThat(s.output().get("body").asText()).isEqualTo("é"));
    }

    @Test
    @DisplayName("T10.extra-1 cancelling the token stops an in-flight HTTP call quickly")
    void cancellation() throws Exception {
        wiremock.stubFor(get("/slow").willReturn(aResponse().withStatus(200).withFixedDelay(5_000)));
        CancellationToken token = new CancellationToken();
        // Cancel once the request has reached the server, so the call is really in flight.
        Thread canceller = Thread.ofVirtual().start(() -> {
            await().atMost(Duration.ofSeconds(5))
                .until(() -> !wiremock.findAll(getRequestedFor(urlEqualTo("/slow"))).isEmpty());
            token.cancel();
        });
        long started = System.nanoTime();
        TaskResult result = executor.execute(context(UUID.randomUUID(), 1, getConfig("/slow"), Map.of(), null, token));
        canceller.join();

        assertThat(result).isInstanceOf(TaskResult.Cancelled.class);
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofMillis(2_000));
    }
}

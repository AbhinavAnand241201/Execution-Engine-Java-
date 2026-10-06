package com.flowforge.core.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.workflow.ValidationError;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DelayTaskExecutorTest {

    private final DelayTaskExecutor executor = new DelayTaskExecutor();

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"{}", "{\"durationMs\": -1}", "{\"durationMs\": \"10\"}", "{\"durationMs\": 1.5}",
        "{\"durationMs\": 3600001}", "{\"durationMs\": 10, \"extra\": 1}"})
    @DisplayName("T05.extra-2 DELAY config must be {durationMs: integer 0..3 600 000} and nothing else")
    void rejectsBadConfig(String json) throws Exception {
        JsonNode config = new ObjectMapper().readTree(json);

        List<ValidationError> errors = executor.validateConfig(config);

        assertThat(errors).isNotEmpty();
    }

    @Test
    @DisplayName("T05.extra-2 a valid DELAY config passes and the task reports how long it slept")
    void sleepsAndSucceeds() throws Exception {
        assertThat(executor.validateConfig(config(20))).isEmpty();

        TaskResult result = executor.execute(context(config(20), new CancellationToken()));

        assertThat(result).isInstanceOfSatisfying(TaskResult.Success.class, success ->
            assertThat(success.output().get("sleptMs").asLong()).isEqualTo(20));
    }

    @Test
    @DisplayName("T05.extra-3 a cancelled token stops a long DELAY quickly")
    void cancelIsObservedQuickly() {
        CancellationToken token = new CancellationToken();
        CompletableFuture<TaskResult> result = CompletableFuture.supplyAsync(() -> {
            try {
                return executor.execute(context(config(10_000), token));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });

        long cancelledAt = System.nanoTime();
        token.cancel();

        await().atMost(Duration.ofMillis(500)).until(result::isDone);
        assertThat(Duration.ofNanos(System.nanoTime() - cancelledAt)).isLessThan(Duration.ofMillis(200));
        assertThat(result.join()).isInstanceOf(TaskResult.Cancelled.class);
    }

    @RepeatedTest(50)
    @DisplayName("T10.9 DELAY honours cancellation within 150 ms (cancelled while really waiting)")
    void cancelWithin150Ms() throws Exception {
        CancellationToken token = new CancellationToken();
        CompletableFuture<TaskResult> result = new CompletableFuture<>();
        Thread worker = Thread.ofPlatform().start(() -> {
            try {
                result.complete(executor.execute(context(config(60_000), token)));
            } catch (Throwable e) {
                result.completeExceptionally(e);
            }
        });
        // Only cancel once the delay is parked, so this measures waking a running DELAY, not an early exit.
        await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(1))
            .until(() -> worker.getState() == Thread.State.TIMED_WAITING);

        long cancelledAt = System.nanoTime();
        token.cancel();

        // get() returns the moment the executor does (no polling interval in the measurement).
        TaskResult outcome = result.get(1, TimeUnit.SECONDS);
        assertThat(Duration.ofNanos(System.nanoTime() - cancelledAt)).isLessThan(Duration.ofMillis(150));
        assertThat(outcome).isInstanceOf(TaskResult.Cancelled.class);
    }

    @Test
    @DisplayName("T05.extra-3 an interrupt ends a DELAY with InterruptedException")
    void honoursInterrupt() {
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                executor.execute(context(config(10_000), new CancellationToken()));
            } catch (Throwable t) {
                thrown.set(t);
            }
        });
        worker.start();
        worker.interrupt();

        await().atMost(Duration.ofSeconds(1)).until(() -> !worker.isAlive());
        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
    }

    @Test
    @DisplayName("T05.extra-4 CancellationToken: callbacks run once, late registrations run immediately, throwIfCancelled throws")
    void cancellationToken() {
        CancellationToken token = new CancellationToken();
        List<String> calls = new CopyOnWriteArrayList<>();
        token.onCancel(() -> calls.add("early"));

        token.cancel();
        token.cancel();
        token.onCancel(() -> calls.add("late"));

        assertThat(token.isCancelled()).isTrue();
        assertThat(calls).containsExactly("early", "late");
        assertThatThrownBy(token::throwIfCancelled).isInstanceOf(CancellationException.class);
    }

    private static JsonNode config(long durationMs) {
        return JsonNodeFactory.instance.objectNode().put("durationMs", durationMs);
    }

    private static TaskContext context(JsonNode config, CancellationToken token) {
        return new TaskContext(UUID.randomUUID(), UUID.randomUUID(), "wait", 1, config, Map.of(),
            Instant.now().plusSeconds(60), token, "test");
    }
}

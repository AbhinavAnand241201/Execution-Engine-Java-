package com.flowforge.worker.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flowforge.core.contracts.RedisKeys;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.runner.TaskRunner;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class HeartbeatServiceTest {

    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> values = mock(ValueOperations.class);
    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    private final AtomicInteger offlineCalls = new AtomicInteger();

    private HeartbeatService service(java.util.function.Supplier<String> ownStatus) {
        when(redis.opsForValue()).thenReturn(values);
        return new HeartbeatService(redis, new ObjectMapper(), mock(TaskRunner.class), new WorkerFreeze(),
            Clock.systemUTC(), "w-1", Set.of(TaskType.DELAY), 2, Duration.ofSeconds(5), Duration.ofSeconds(15),
            drain -> { }, ownStatus, offlineCalls::incrementAndGet);
    }

    @Test
    @DisplayName("T16.extra-2 the database being down doesn't stop the heartbeat (a live worker must not look dead)")
    void databaseDownStillBeats() {
        HeartbeatService service = service(() -> {
            throw new IllegalStateException("connection refused");
        });

        service.beat();

        verify(values).set(eq(RedisKeys.heartbeat("w-1")), anyString(), any(Duration.class));
        assertThat(offlineCalls).hasValue(0);
    }

    @Test
    @DisplayName("T16.extra-3 a worker that reads its own status as OFFLINE stops, once")
    void declaredOfflineStopsOnce() {
        HeartbeatService service = service(() -> "OFFLINE");

        service.beat();
        service.beat();

        assertThat(offlineCalls).hasValue(1);
    }
}

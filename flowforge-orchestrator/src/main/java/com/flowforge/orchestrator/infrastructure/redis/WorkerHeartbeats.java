package com.flowforge.orchestrator.infrastructure.redis;

import com.flowforge.core.contracts.RedisKeys;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/** The orchestrator's view of worker liveness in Redis: heartbeat keys and drain flags. */
public class WorkerHeartbeats {

    private final StringRedisTemplate redis;

    public WorkerHeartbeats(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** True if Redis answers. Without it a missing heartbeat means nothing. */
    public boolean available() {
        try {
            String pong = redis.execute((RedisCallback<String>) connection -> connection.ping());
            return "PONG".equalsIgnoreCase(pong);
        } catch (RuntimeException e) {
            return false;
        }
    }

    /**
     * Heartbeat payloads of these workers in one round trip (MGET); a missing entry = no live heartbeat. Empty if
     * Redis gave no answer at all (then nothing can be concluded).
     */
    public Optional<Map<String, String>> heartbeats(List<String> workerIds) {
        Map<String, String> found = new HashMap<>();
        if (workerIds.isEmpty()) {
            return Optional.of(found);
        }
        List<String> values;
        try {
            values = redis.opsForValue().multiGet(workerIds.stream().map(RedisKeys::heartbeat).toList());
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        if (values == null) {
            return Optional.empty();
        }
        for (int i = 0; i < workerIds.size(); i++) {
            String value = values.get(i);
            if (value != null) {
                found.put(workerIds.get(i), value);
            }
        }
        return Optional.of(found);
    }

    public void requestDrain(String workerId, Duration ttl) {
        redis.opsForValue().set(RedisKeys.drain(workerId), "1", ttl);
    }
}

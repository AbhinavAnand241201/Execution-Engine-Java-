package com.flowforge.worker.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flowforge.core.contracts.RedisKeys;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.runtime.runner.TaskRunner;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * "I'm alive" every {@code interval}: {@code SET ff:worker:{id}:hb <json> EX ttl}. If the worker stops
 * (crash, freeze, partition) the key expires after the TTL and the orchestrator's WorkerMonitor declares it OFFLINE.
 * Each beat also checks the drain flag set by an operator, and the worker's own registry status. A failed beat (Redis
 * down) is logged and retried next time: the monitor skips its checks while Redis is down, so nobody is declared dead
 * for Redis's sake.
 */
public class HeartbeatService {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatService.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper json;
    private final TaskRunner runner;
    private final WorkerFreeze freeze;
    private final Clock clock;
    private final String workerId;
    private final Set<TaskType> capabilities;
    private final int maxConcurrency;
    private final Duration interval;
    private final Duration ttl;
    private final Consumer<Boolean> onDrainFlag;
    private final Supplier<String> ownStatus;
    private final Runnable onDeclaredOffline;
    private volatile boolean declaredOffline;
    private ScheduledExecutorService scheduler;

    public HeartbeatService(StringRedisTemplate redis, ObjectMapper json, TaskRunner runner, WorkerFreeze freeze,
                            Clock clock, String workerId, Set<TaskType> capabilities, int maxConcurrency,
                            Duration interval, Duration ttl, Consumer<Boolean> onDrainFlag,
                            Supplier<String> ownStatus, Runnable onDeclaredOffline) {
        this.redis = redis;
        this.json = json;
        this.runner = runner;
        this.freeze = freeze;
        this.clock = clock;
        this.workerId = workerId;
        this.capabilities = Set.copyOf(capabilities);
        this.maxConcurrency = maxConcurrency;
        this.interval = interval;
        this.ttl = ttl;
        this.onDrainFlag = onDrainFlag;
        this.ownStatus = ownStatus;
        this.onDeclaredOffline = onDeclaredOffline;
    }

    public synchronized void start() {
        beat();
        scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().name("ff-heartbeat").daemon()
            .factory());
        scheduler.scheduleWithFixedDelay(this::beat, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    void beat() {
        if (freeze.isFrozen() || declaredOffline) {
            return;
        }
        try {
            ObjectNode payload = json.createObjectNode().put("load", runner.inFlight())
                .put("maxConcurrency", maxConcurrency).put("ts", clock.instant().toString());
            capabilities.stream().map(Enum::name).sorted().forEach(payload.putArray("capabilities")::add);
            redis.opsForValue().set(RedisKeys.heartbeat(workerId), payload.toString(), ttl);
            onDrainFlag.accept(Boolean.TRUE.equals(redis.hasKey(RedisKeys.drain(workerId))));
        } catch (RuntimeException e) {
            log.warn("heartbeat failed workerId={} error={}", workerId, e.toString());
        }
        checkOwnStatus();
    }

    /**
     * The registry is the record: if the orchestrator already declared this incarnation OFFLINE (it missed its
     * heartbeats: GC pause, network, frozen container), its attempts were recovered and it must not keep working as
     * an invisible zombie. Done after the heartbeat and in its own try, so a database outage can't stop the beats
     * (that would get live workers declared dead).
     */
    private void checkOwnStatus() {
        try {
            if ("OFFLINE".equals(ownStatus.get())) {
                declaredOffline = true;
                log.error("this worker was declared OFFLINE workerId={}; stopping", workerId);
                onDeclaredOffline.run();
            }
        } catch (RuntimeException e) {
            log.warn("own status check failed workerId={} error={}", workerId, e.toString());
        }
    }

    /** Stops beating; {@code removeKey} = a graceful exit (the key goes at once instead of expiring). */
    public synchronized void stop(boolean removeKey) {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (removeKey) {
            try {
                redis.delete(RedisKeys.heartbeat(workerId));
            } catch (RuntimeException e) {
                log.warn("could not delete heartbeat key workerId={} error={}", workerId, e.toString());
            }
        }
    }
}

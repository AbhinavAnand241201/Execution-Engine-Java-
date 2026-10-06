package com.flowforge.core.testing;

import com.fasterxml.jackson.databind.JsonNode;
import com.flowforge.core.executor.TaskContext;
import com.flowforge.core.executor.TaskExecutor;
import com.flowforge.core.executor.TaskResult;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test executor whose behaviour is chosen per task key (behaviours live in {@link Behaviours}), and
 * which records what really happened on the worker threads: start order, invocations, start/finish times and the
 * highest number of tasks running at once. Tests assert on these records, independently of the engine's own report.
 */
public final class ScriptedExecutor implements TaskExecutor {

    /** What a task does when it runs. */
    @FunctionalInterface
    public interface Behaviour {
        TaskResult run(TaskContext context) throws InterruptedException;
    }

    private final TaskType type;
    private final Behaviour fallback;
    private final Map<String, Behaviour> byKey = new ConcurrentHashMap<>();

    private final Queue<String> startOrder = new ConcurrentLinkedQueue<>();
    private final Queue<String> finishOrder = new ConcurrentLinkedQueue<>();
    private final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();
    private final Map<String, Long> firstStartNanos = new ConcurrentHashMap<>();
    private final Map<String, Long> lastFinishNanos = new ConcurrentHashMap<>();
    private final AtomicInteger running = new AtomicInteger();
    private final AtomicInteger maxRunning = new AtomicInteger();

    public ScriptedExecutor(TaskType type, Behaviour fallback) {
        this.type = type;
        this.fallback = fallback;
    }

    /** DELAY-typed executor whose tasks succeed immediately unless told otherwise. */
    public static ScriptedExecutor succeeding() {
        return new ScriptedExecutor(TaskType.DELAY, Behaviours.succeed());
    }

    public ScriptedExecutor on(String taskKey, Behaviour behaviour) {
        byKey.put(taskKey, behaviour);
        return this;
    }

    @Override
    public TaskType supportedType() {
        return type;
    }

    @Override
    public List<ValidationError> validateConfig(JsonNode config) {
        return List.of();
    }

    @Override
    public TaskResult execute(TaskContext context) throws InterruptedException {
        String key = context.taskKey();
        int now = running.incrementAndGet();
        maxRunning.accumulateAndGet(now, Math::max);
        invocations.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
        firstStartNanos.putIfAbsent(key, System.nanoTime());
        startOrder.add(key);
        try {
            return byKey.getOrDefault(key, fallback).run(context);
        } finally {
            lastFinishNanos.put(key, System.nanoTime());
            finishOrder.add(key);
            running.decrementAndGet();
        }
    }

    public List<String> startOrder() {
        return List.copyOf(startOrder);
    }

    public List<String> finishOrder() {
        return List.copyOf(finishOrder);
    }

    public int invocations(String key) {
        AtomicInteger count = invocations.get(key);
        return count == null ? 0 : count.get();
    }

    public int maxConcurrent() {
        return maxRunning.get();
    }

    public long firstStartNanos(String key) {
        return firstStartNanos.get(key);
    }

    public long lastFinishNanos(String key) {
        return lastFinishNanos.get(key);
    }
}

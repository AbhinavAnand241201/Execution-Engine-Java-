package com.flowforge.core.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.flowforge.core.workflow.TaskType;
import com.flowforge.core.workflow.ValidationError;
import com.flowforge.core.workflow.ValidationErrorCode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Waits for {@code durationMs}. Useful for demos, tests and "wait before the next step" workflows. It waits on a
 * latch that its cancellation token counts down, so a cancel wakes it at once even when nobody interrupts the thread.
 *
 * <p>The wait is a real, monotonic one ({@code CountDownLatch.await} uses {@code System.nanoTime}), which is different
 * from the wall-clock timestamps for which FlowForge injects a {@code Clock}.
 */
public final class DelayTaskExecutor implements TaskExecutor {

    static final long MAX_DURATION_MS = 3_600_000;

    @Override
    public TaskType supportedType() {
        return TaskType.DELAY;
    }

    @Override
    public List<ValidationError> validateConfig(JsonNode config) {
        List<ValidationError> errors = new ArrayList<>();
        JsonNode duration = config.get("durationMs");
        if (duration == null || !duration.isIntegralNumber()) {
            errors.add(invalid("durationMs", "durationMs is required and must be a whole number of milliseconds"));
        } else if (duration.asLong() < 0 || duration.asLong() > MAX_DURATION_MS) {
            errors.add(invalid("durationMs", "durationMs must be between 0 and " + MAX_DURATION_MS));
        }
        config.fieldNames().forEachRemaining(field -> {
            if (!field.equals("durationMs")) {
                errors.add(invalid(field, "unknown field '" + field + "'"));
            }
        });
        return errors;
    }

    @Override
    public TaskResult execute(TaskContext context) throws InterruptedException {
        long durationMs = context.config().get("durationMs").asLong();
        CountDownLatch cancelled = new CountDownLatch(1);
        context.cancellation().onCancel(cancelled::countDown);
        // Wakes the moment the token is cancelled (or the thread interrupted), otherwise after durationMs.
        if (cancelled.await(durationMs, TimeUnit.MILLISECONDS)) {
            return TaskResult.cancelled();
        }
        return TaskResult.success(JsonNodeFactory.instance.objectNode().put("sleptMs", durationMs));
    }

    private static ValidationError invalid(String path, String message) {
        return new ValidationError(path, ValidationErrorCode.INVALID_TASK_CONFIG, message);
    }
}

package com.flowforge.runtime.executor;

import com.flowforge.core.executor.TaskResult;
import java.nio.charset.StandardCharsets;

/**
 * Rejects outputs too large to store (64 KB). Outputs are copied into every dependent task's context and
 * kept in {@code task_executions.output}; big payloads belong in object storage with a reference passed instead
 * (the claim-check pattern). The task runner applies this to every success before reporting it.
 */
public final class OutputGuard {

    private final int maxBytes;

    public OutputGuard(int maxBytes) {
        this.maxBytes = maxBytes;
    }

    public TaskResult check(TaskResult result) {
        if (result instanceof TaskResult.Success success && success.output() != null) {
            int size = success.output().toString().getBytes(StandardCharsets.UTF_8).length;
            if (size > maxBytes) {
                return TaskResult.failure("OUTPUT_TOO_LARGE", "output is " + size + " bytes; the limit is " + maxBytes,
                    false);
            }
        }
        return result;
    }
}

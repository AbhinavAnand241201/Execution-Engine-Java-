package com.flowforge.core.workflow;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Checks the structural rules of a workflow definition and <strong>collects every error</strong> rather than stopping at the
 * first, so a user fixes a definition in one round trip. Cycle detection is added by composition; executor
 * config rules come in through {@link ConfigValidator}.
 */
public class WorkflowDefinitionValidator {

    public static final int DEFAULT_MAX_TASKS = 200;

    static final Pattern KEY_PATTERN = Pattern.compile("^[a-z0-9][a-z0-9-]{0,63}$");
    static final Duration MIN_TIMEOUT = Duration.ofMillis(100);
    static final Duration MAX_TIMEOUT = Duration.ofMillis(3_600_000);
    static final int MIN_ATTEMPTS = 1;
    static final int MAX_ATTEMPTS = 10;
    static final Duration MAX_INITIAL_DELAY = Duration.ofMillis(600_000);
    static final Duration MAX_DELAY = Duration.ofMillis(3_600_000);
    static final double MIN_MULTIPLIER = 1.0;
    static final double MAX_MULTIPLIER = 10.0;

    private final ConfigValidator configValidator;
    private final int maxTasks;

    public WorkflowDefinitionValidator() {
        this(ConfigValidator.ACCEPT_ALL, DEFAULT_MAX_TASKS);
    }

    public WorkflowDefinitionValidator(ConfigValidator configValidator, int maxTasks) {
        if (maxTasks < 1) {
            throw new IllegalArgumentException("maxTasks must be >= 1: " + maxTasks);
        }
        this.configValidator = Objects.requireNonNull(configValidator, "configValidator");
        this.maxTasks = maxTasks;
    }

    /** Returns every problem found, or an empty list if the definition is structurally valid. */
    public List<ValidationError> validate(WorkflowDefinition definition) {
        List<ValidationError> errors = new ArrayList<>();
        List<TaskDefinition> tasks = definition.tasks();

        if (tasks.isEmpty()) {
            errors.add(new ValidationError("tasks", ValidationErrorCode.TOO_FEW_TASKS, "at least 1 task is required"));
        } else if (tasks.size() > maxTasks) {
            errors.add(new ValidationError("tasks", ValidationErrorCode.TOO_MANY_TASKS,
                "at most " + maxTasks + " tasks are allowed, got " + tasks.size()));
        }

        Set<String> allKeys = new HashSet<>();
        for (TaskDefinition task : tasks) {
            if (task.key() != null) {
                allKeys.add(task.key());
            }
        }

        Set<String> seenKeys = new HashSet<>();
        for (int i = 0; i < tasks.size(); i++) {
            validateTask(tasks.get(i), "tasks[" + i + "]", allKeys, seenKeys, errors);
        }
        return List.copyOf(errors);
    }

    private void validateTask(TaskDefinition task, String path, Set<String> allKeys, Set<String> seenKeys,
                              List<ValidationError> errors) {
        String key = task.key();
        if (key == null) {
            errors.add(new ValidationError(path + ".key", ValidationErrorCode.REQUIRED, "key is required"));
        } else if (!KEY_PATTERN.matcher(key).matches()) {
            errors.add(new ValidationError(path + ".key", ValidationErrorCode.INVALID_TASK_KEY,
                "key must match " + KEY_PATTERN.pattern()));
        } else if (!seenKeys.add(key)) {
            errors.add(new ValidationError(path + ".key", ValidationErrorCode.DUPLICATE_TASK_KEY,
                "duplicate task key '" + key + "'"));
        }

        if (task.type() == null) {
            errors.add(new ValidationError(path + ".type", ValidationErrorCode.REQUIRED, "type is required"));
        }
        if (!task.config().isObject()) {
            errors.add(new ValidationError(path + ".config", ValidationErrorCode.INVALID_TASK_CONFIG, "config must be an object"));
        } else if (task.type() != null) {
            // Executors can then rely on receiving an object.
            for (ValidationError configError : configValidator.validate(task.type(), task.config())) {
                errors.add(configError.under(path + ".config"));
            }
        }

        validateDependencies(task, path, allKeys, errors);

        if (outside(task.timeout(), MIN_TIMEOUT, MAX_TIMEOUT)) {
            errors.add(outOfRange(path + ".timeoutMs", MIN_TIMEOUT.toMillis(), MAX_TIMEOUT.toMillis()));
        }
        validateRetry(task.retry(), path + ".retry", errors);
    }

    private static void validateDependencies(TaskDefinition task, String path, Set<String> allKeys,
                                             List<ValidationError> errors) {
        Set<String> seen = new HashSet<>();
        for (int j = 0; j < task.dependsOn().size(); j++) {
            String dependency = task.dependsOn().get(j);
            String depPath = path + ".dependsOn[" + j + "]";
            if (dependency.equals(task.key())) {
                errors.add(new ValidationError(depPath, ValidationErrorCode.SELF_DEPENDENCY,
                    "task '" + dependency + "' depends on itself"));
            } else if (!seen.add(dependency)) {
                errors.add(new ValidationError(depPath, ValidationErrorCode.DUPLICATE_DEPENDENCY,
                    "'" + dependency + "' is listed more than once"));
            } else if (!allKeys.contains(dependency)) {
                errors.add(new ValidationError(depPath, ValidationErrorCode.UNKNOWN_DEPENDENCY,
                    "no task with key '" + dependency + "'"));
            }
        }
    }

    private static void validateRetry(RetryPolicySpec retry, String path, List<ValidationError> errors) {
        if (retry.maxAttempts() < MIN_ATTEMPTS || retry.maxAttempts() > MAX_ATTEMPTS) {
            errors.add(outOfRange(path + ".maxAttempts", MIN_ATTEMPTS, MAX_ATTEMPTS));
        }
        boolean initialDelayValid = !outside(retry.initialDelay(), Duration.ZERO, MAX_INITIAL_DELAY);
        if (!initialDelayValid) {
            errors.add(outOfRange(path + ".initialDelayMs", 0, MAX_INITIAL_DELAY.toMillis()));
        }
        // Compared with initialDelay only when that is valid, so one bad field isn't blamed on the default maxDelay.
        Duration maxDelayFloor = initialDelayValid ? retry.initialDelay() : Duration.ZERO;
        if (outside(retry.maxDelay(), maxDelayFloor, MAX_DELAY)) {
            errors.add(new ValidationError(path + ".maxDelayMs", ValidationErrorCode.OUT_OF_RANGE,
                "must be >= initialDelayMs (" + maxDelayFloor.toMillis() + ") and <= " + MAX_DELAY.toMillis()));
        }
        // A non-finite multiplier is rejected for every backoff type so it never reaches the stored JSON.
        if (!Double.isFinite(retry.multiplier())) {
            errors.add(new ValidationError(path + ".multiplier", ValidationErrorCode.OUT_OF_RANGE, "must be a finite number"));
        } else if (retry.backoff() == BackoffType.EXPONENTIAL
            && (retry.multiplier() < MIN_MULTIPLIER || retry.multiplier() > MAX_MULTIPLIER)) {
            errors.add(new ValidationError(path + ".multiplier", ValidationErrorCode.OUT_OF_RANGE,
                "must be between " + MIN_MULTIPLIER + " and " + MAX_MULTIPLIER));
        }
    }

    private static boolean outside(Duration value, Duration min, Duration max) {
        return value.compareTo(min) < 0 || value.compareTo(max) > 0;
    }

    private static ValidationError outOfRange(String path, long min, long max) {
        return new ValidationError(path, ValidationErrorCode.OUT_OF_RANGE, "must be between " + min + " and " + max);
    }
}

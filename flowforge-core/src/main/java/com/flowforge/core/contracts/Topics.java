package com.flowforge.core.contracts;

import com.flowforge.core.workflow.TaskType;
import java.util.Locale;

/** Kafka topic and header names, shared by the orchestrator and the worker so they can't drift apart. */
public final class Topics {

    public static final String TASK_RESULTS = "ff.task.results";
    public static final String WORKFLOW_EVENTS = "ff.workflow.events";
    public static final String WORKER_EVENTS = "ff.worker.events";
    public static final String DEAD_LETTER_SUFFIX = ".DLT";

    public static final String HEADER_MESSAGE_TYPE = "ff-message-type";
    public static final String HEADER_CORRELATION_ID = "ff-correlation-id";
    public static final String HEADER_MESSAGE_ID = "ff-message-id";

    private Topics() {
    }

    /** One command topic per task type, so only workers with that capability receive it. */
    public static String taskCommands(TaskType type) {
        return "ff.task.commands." + type.name().toLowerCase(Locale.ROOT);
    }
}

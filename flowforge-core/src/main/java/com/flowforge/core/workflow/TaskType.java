package com.flowforge.core.workflow;

/**
 * The kinds of work a task can do. Each type has exactly one executor and its own Kafka topic, so a worker only
 * receives the types it can run.
 */
public enum TaskType {
    HTTP,
    DELAY,
    DATABASE,
    REPORT
}

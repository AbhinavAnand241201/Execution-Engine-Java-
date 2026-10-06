package com.flowforge.core.engine;

/**
 * Which threads run tasks. PLATFORM uses a fixed pool of OS threads; VIRTUAL starts one virtual thread per task,
 * with a semaphore bounding how many run at once.
 */
public enum ExecutionMode {
    PLATFORM,
    VIRTUAL
}

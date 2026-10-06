package com.flowforge.runtime.runner;

/** How task attempts get threads; the capacity semaphore bounds both ({@code thread-mode}). */
public enum ThreadMode {
    /** A fixed pool of named platform threads, one per permit. */
    PLATFORM,
    /** A new virtual thread per attempt; cheap while tasks block on I/O. */
    VIRTUAL
}

package com.flowforge.runtime.runner;

/** What the caller should do with the command it delivered. */
public enum CommandOutcome {
    /** Handled: claimed and started, or dropped as a duplicate/stale/paused claim. Acknowledge the message. */
    ACK,
    /** No capacity right now; deliver the command again later. Nothing was claimed. */
    RETRY_LATER
}

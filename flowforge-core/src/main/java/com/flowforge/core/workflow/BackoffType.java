package com.flowforge.core.workflow;

/** How the wait between retries grows: constant, by a fixed step, or geometrically. */
public enum BackoffType {
    FIXED,
    LINEAR,
    EXPONENTIAL
}

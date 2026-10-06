package com.flowforge.worker.config;

/** This process's worker id, {@code <name>-<8 hex>}: a new incarnation on every start (workers.id). */
public record WorkerIdentity(String id, String name) {
}

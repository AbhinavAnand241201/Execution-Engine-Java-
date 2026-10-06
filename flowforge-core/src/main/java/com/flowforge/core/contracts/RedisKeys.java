package com.flowforge.core.contracts;

import java.util.UUID;

/** Redis key names, shared by the orchestrator and the worker. Redis is never the only record of anything. */
public final class RedisKeys {

    private RedisKeys() {
    }

    /** JSON {load, maxConcurrency, capabilities, ts}, TTL = heartbeat TTL: present ⇔ the worker is alive. */
    public static String heartbeat(String workerId) {
        return "ff:worker:" + workerId + ":hb";
    }

    /** "1" while an operator wants this worker drained (the DB status DRAINING is the record). */
    public static String drain(String workerId) {
        return "ff:worker:" + workerId + ":drain";
    }

    /** Cooperative cancel signal for an execution's running tasks. */
    public static String cancel(UUID workflowExecutionId) {
        return "ff:cancel:" + workflowExecutionId;
    }
}

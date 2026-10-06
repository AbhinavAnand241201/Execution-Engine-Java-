package com.flowforge.core.engine;

import com.flowforge.core.executor.CancellationToken;

/**
 * Lets the coordinator stop one specific attempt. Interrupting "the thread that ran it" is unsafe on a pool: by the
 * time the interrupt arrives the thread may be running a different task. So the interrupt is only delivered while
 * this attempt is running, and the worker clears any late interrupt before its thread moves on.
 */
final class AttemptControl {

    final CancellationToken token = new CancellationToken();
    private Thread thread;
    private boolean finished;

    /** Called by the worker thread when the attempt starts. */
    synchronized void bind(Thread worker) {
        this.thread = worker;
    }

    /** Called by the worker thread when the attempt ends; swallows an interrupt meant for this attempt only. */
    synchronized void finish() {
        finished = true;
        Thread.interrupted();
    }

    /** Called by the coordinator (timeout, shutdown): cooperative cancel plus an interrupt if still running. */
    synchronized void cancelAndInterrupt() {
        token.cancel();
        if (thread != null && !finished) {
            thread.interrupt();
        }
    }
}

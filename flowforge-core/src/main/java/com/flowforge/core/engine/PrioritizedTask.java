package com.flowforge.core.engine;

/**
 * A unit of work that orders itself: higher priority weight first, then first-come-first-served within a priority.
 *
 * <p>It must reach the {@code PriorityBlockingQueue} as itself. {@code ExecutorService.submit()} and
 * {@code CompletableFuture.supplyAsync(..., pool)} wrap tasks in their own non-{@code Comparable} objects, which a
 * priority queue rejects with a {@code ClassCastException}; so the engine uses {@code execute()} and this class
 * reports its own completion instead of returning a {@code Future}.
 */
final class PrioritizedTask implements Runnable, Comparable<PrioritizedTask> {

    private final int priorityWeight;
    private final long sequence;
    private final Runnable body;

    PrioritizedTask(int priorityWeight, long sequence, Runnable body) {
        this.priorityWeight = priorityWeight;
        this.sequence = sequence;
        this.body = body;
    }

    @Override
    public void run() {
        body.run();
    }

    @Override
    public int compareTo(PrioritizedTask other) {
        int byPriority = Integer.compare(other.priorityWeight, priorityWeight);
        return byPriority != 0 ? byPriority : Long.compare(sequence, other.sequence);
    }
}

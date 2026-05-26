package org.purpurmc.purpur.thread;

import org.purpurmc.purpur.thread.annotation.MainThreadOnly;
import org.purpurmc.purpur.thread.annotation.ThreadSafe;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A lock-free, bounded-warning queue that buffers tasks submitted by region threads (or
 * any non-main thread) and drains them synchronously on the main server thread each tick.
 *
 * <h3>Purpose — plugin compatibility layer</h3>
 * When a plugin's event handler fires from inside a parallel world tick, it may call
 * Bukkit API that is normally {@link MainThreadOnly} (e.g., scheduling a new task via
 * {@code Bukkit.getScheduler().runTask(...)}, spawning an entity, or modifying an
 * inventory).  Instead of crashing with {@link IllegalStateException}, the plugin (or a
 * Purpur compatibility shim) should route the call through this queue:
 *
 * <pre>{@code
 *   // from inside a region thread:
 *   TaskQueue.getInstance().submit(() -> {
 *       // executes on the main thread next drain cycle
 *       player.teleport(destination);
 *   });
 * }</pre>
 *
 * <h3>Drain timing</h3>
 * {@link MultiThreadedTicker#tickWorlds} calls {@link #drainAll()} after all world ticks
 * complete and before the main server loop continues.  This means tasks queued during a
 * tick are processed within the <em>same tick</em> before any packet send / network flush.
 *
 * <h3>Backpressure warning</h3>
 * If the queue exceeds {@value #WARN_BACKLOG_THRESHOLD} items, a warning is logged every
 * time the threshold is crossed again.  This usually indicates a plugin that is
 * generating far too many main-thread tasks per tick.
 */
@ThreadSafe
public final class TaskQueue {

    private static final Logger LOGGER = Logger.getLogger("Purpur-TaskQueue");

    /** Log a warning whenever the pending count passes this multiple. */
    private static final int WARN_BACKLOG_THRESHOLD = 500;

    /** Singleton instance. */
    private static final TaskQueue INSTANCE = new TaskQueue();

    /**
     * The backing queue.  {@link ConcurrentLinkedQueue} is wait-free for concurrent
     * producers (region threads) and safe for a single draining consumer (main thread).
     */
    private final ConcurrentLinkedQueue<PendingTask<?>> queue = new ConcurrentLinkedQueue<>();

    // ── metrics ──────────────────────────────────────────────────────────────
    private volatile long totalDrained = 0L;
    private volatile int lastDrainCount = 0;

    private TaskQueue() {}

    /** Returns the singleton {@code TaskQueue}. */
    public static TaskQueue getInstance() {
        return INSTANCE;
    }

    // ──────────────────────────────────────────────────────────── submission ──

    /**
     * Enqueues a {@link Callable} to be executed on the main thread during the next
     * {@link #drain} call.
     *
     * @param callable the work to perform; must not be {@code null}
     * @param <T>      the result type
     * @return a {@link CompletableFuture} that will be completed by the main thread once
     *         the callable executes; completed exceptionally if the callable throws
     */
    public <T> CompletableFuture<T> submit(final Callable<T> callable) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        queue.add(new PendingTask<>(callable, future));
        checkBacklog();
        return future;
    }

    /**
     * Enqueues a {@link Runnable} to be executed on the main thread.
     *
     * @param runnable the work to perform; must not be {@code null}
     * @return a {@link CompletableFuture}{@code <Void>} completed after the runnable runs
     */
    public CompletableFuture<Void> submit(final Runnable runnable) {
        return submit(() -> {
            runnable.run();
            return (Void) null;
        });
    }

    // ──────────────────────────────────────────────────────────────── drain ──

    /**
     * Drains <em>all</em> currently queued tasks, executing each synchronously on the
     * calling thread (which must be the main thread).
     *
     * <p>Tasks added to the queue <em>during</em> this drain are <strong>not</strong>
     * processed in the same call; they are picked up in the next drain cycle.  This
     * prevents an unbounded drain loop if tasks recursively submit new tasks.
     *
     * @return the number of tasks executed
     */
    @MainThreadOnly
    public int drainAll() {
        return drain(Integer.MAX_VALUE);
    }

    /**
     * Drains up to {@code maxTasks} queued tasks.  Useful for distributing drain work
     * across multiple tick phases if the backlog is very large.
     *
     * @param maxTasks the maximum number of tasks to execute in this call (≥ 0)
     * @return the number of tasks actually executed
     */
    @MainThreadOnly
    public int drain(final int maxTasks) {
        if (maxTasks <= 0) return 0;

        int count = 0;
        PendingTask<?> task;
        while (count < maxTasks && (task = queue.poll()) != null) {
            executeTask(task);
            count++;
        }

        lastDrainCount = count;
        totalDrained += count;
        return count;
    }

    // ──────────────────────────────────────────────────────────── metrics ──

    /** Returns the number of tasks currently waiting to be drained. */
    @ThreadSafe
    public int pendingCount() {
        return queue.size();
    }

    /** Returns the number of tasks drained in the most recent {@link #drain} call. */
    @ThreadSafe
    public int lastDrainCount() {
        return lastDrainCount;
    }

    /** Returns the cumulative number of tasks drained since server start. */
    @ThreadSafe
    public long totalDrained() {
        return totalDrained;
    }

    // ────────────────────────────────────────────────────────────── internal ──

    @SuppressWarnings("unchecked")
    private <T> void executeTask(final PendingTask<T> task) {
        try {
            final T result = task.callable().call();
            task.future().complete(result);
        } catch (final Throwable t) {
            task.future().completeExceptionally(t);
            LOGGER.log(Level.WARNING, "[TaskQueue] Uncaught exception in main-thread task", t);
        }
    }

    private void checkBacklog() {
        final int size = queue.size();
        if (size > WARN_BACKLOG_THRESHOLD && (size % WARN_BACKLOG_THRESHOLD) == 1) {
            LOGGER.warning(String.format(
                    "[TaskQueue] Backlog has grown to %d pending tasks. "
                            + "A plugin may be submitting excessive main-thread work per tick. "
                            + "Consider adjusting multi-thread settings or profiling plugins.",
                    size));
        }
    }

    // ──────────────────────────────────────────────────────────── record ──

    /** Holds a callable and the future that should be resolved when it completes. */
    private record PendingTask<T>(Callable<T> callable, CompletableFuture<T> future) {}
}

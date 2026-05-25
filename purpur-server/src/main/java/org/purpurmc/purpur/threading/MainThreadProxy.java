package org.purpurmc.purpur.threading;

import org.purpurmc.purpur.threading.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Main Thread Proxy — manages a task queue that is drained by the main server thread.
 * <p>
 * Region worker threads submit tasks to this proxy when they need an operation
 * to execute on the main thread (e.g., Bukkit API calls that require main thread access).
 * The main server thread continuously drains this queue while waiting for region ticks
 * to complete, preventing deadlocks.
 * </p>
 *
 * <h3>Usage from Region threads:</h3>
 * <pre>{@code
 * // Fire-and-forget (async)
 * MainThreadProxy.submit(() -> someMainThreadOperation());
 *
 * // Synchronous wait for result
 * BlockState state = MainThreadProxy.submitAndWait(() -> world.getBlockState(pos));
 * }</pre>
 *
 * <h3>Usage from Main thread (in tick loop):</h3>
 * <pre>{@code
 * // Drain all pending proxy requests
 * MainThreadProxy.drainQueue(remainingNanos);
 * }</pre>
 */
public final class MainThreadProxy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MainThreadProxy.class);

    /**
     * The pending task queue. Region threads enqueue tasks; main thread dequeues and executes them.
     * Using ConcurrentLinkedQueue for lock-free enqueue/dequeue to minimize contention.
     */
    private static final ConcurrentLinkedQueue<PendingTask<?>> TASK_QUEUE = new ConcurrentLinkedQueue<>();

    /** Total number of tasks proxied since server start (monitoring metric) */
    private static final AtomicLong TOTAL_PROXIED_CALLS = new AtomicLong(0);

    /** Peak queue size observed (monitoring metric) */
    private static volatile long peakQueueSize = 0;

    private MainThreadProxy() {} // No instantiation

    /**
     * Submits a task to be executed on the main thread (fire-and-forget).
     * <p>
     * The caller does NOT wait for the task to complete. Use {@link #submitAndWait(Supplier)}
     * if you need to wait for the result.
     * </p>
     *
     * @param task the task to execute on the main thread
     * @return a CompletableFuture that completes when the task has been executed
     */
    @ThreadSafe("Lock-free ConcurrentLinkedQueue")
    public static CompletableFuture<Void> submit(Runnable task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        TASK_QUEUE.add(new PendingTask<>(() -> {
            task.run();
            return null;
        }, future));
        TOTAL_PROXIED_CALLS.incrementAndGet();
        return future;
    }

    /**
     * Submits a task to be executed on the main thread and returns a future for its result.
     *
     * @param task the task to execute on the main thread
     * @param <T>  the return type of the task
     * @return a CompletableFuture that completes with the task's result
     */
    @ThreadSafe("Lock-free ConcurrentLinkedQueue")
    public static <T> CompletableFuture<T> submit(Supplier<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        TASK_QUEUE.add(new PendingTask<>(task, future));
        TOTAL_PROXIED_CALLS.incrementAndGet();
        return future;
    }

    /**
     * Submits a task to the main thread and blocks the calling thread until it completes.
     * <p>
     * <b>WARNING:</b> This method MUST NOT be called from the main thread itself,
     * as it would cause a deadlock (the main thread would wait for itself to drain the queue).
     * </p>
     *
     * @param task the task to execute on the main thread
     * @param <T>  the return type of the task
     * @return the result of the task
     * @throws IllegalStateException if called from the main thread
     * @throws RuntimeException      if the task throws an exception
     */
    @ThreadSafe
    public static <T> T submitAndWait(Supplier<T> task) {
        if (ThreadContext.isMainThread()) {
            // If we're already on the main thread, just execute directly
            return task.get();
        }

        CompletableFuture<T> future = submit(task);
        try {
            long timeoutMs = org.purpurmc.purpur.PurpurConfig.threadProxyTimeoutMs;
            return future.get(timeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.TimeoutException e) {
            String msg = String.format(
                "[ThreadProxy] Task timed out after %dms waiting for main thread execution. " +
                "Current thread: %s. This may indicate the main thread is overloaded or deadlocked.",
                org.purpurmc.purpur.PurpurConfig.threadProxyTimeoutMs,
                Thread.currentThread().getName()
            );
            if (org.purpurmc.purpur.PurpurConfig.threadProxyWarnOnly) {
                LOGGER.warn(msg);
                // Execute on current thread as fallback (risky but prevents server hang)
                LOGGER.warn("[ThreadProxy] Executing task on current thread as fallback. This may cause data inconsistency!");
                return task.get();
            } else {
                throw new RuntimeException(msg, e);
            }
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException("[ThreadProxy] Task execution failed on main thread", e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("[ThreadProxy] Interrupted while waiting for main thread", e);
        }
    }

    /**
     * Submits a Runnable to the main thread and blocks until it completes.
     *
     * @param task the task to execute on the main thread
     * @throws IllegalStateException if called from the main thread
     */
    @ThreadSafe
    public static void submitAndWait(Runnable task) {
        submitAndWait(() -> {
            task.run();
            return null;
        });
    }

    /**
     * Drains the task queue, executing tasks on the main thread.
     * <p>
     * This method should be called from the main server thread, typically
     * during the tick loop while waiting for region worker threads to complete.
     * </p>
     *
     * @param maxNanos the maximum time in nanoseconds to spend draining.
     *                 Pass 0 or negative to drain all pending tasks regardless of time.
     * @return the number of tasks executed
     */
    public static int drainQueue(long maxNanos) {
        int executed = 0;
        long startNanos = System.nanoTime();

        PendingTask<?> pendingTask;
        while ((pendingTask = TASK_QUEUE.poll()) != null) {
            executePendingTask(pendingTask);
            executed++;

            // Check time limit (if specified)
            if (maxNanos > 0 && (System.nanoTime() - startNanos) >= maxNanos) {
                break;
            }
        }

        // Update peak queue size metric
        int remaining = approximateQueueSize();
        if (remaining > peakQueueSize) {
            peakQueueSize = remaining;
        }

        return executed;
    }

    /**
     * Executes a single pending task, completing its future with the result or exception.
     */
    @SuppressWarnings("unchecked")
    private static <T> void executePendingTask(PendingTask<T> pendingTask) {
        try {
            T result = pendingTask.task.get();
            pendingTask.future.complete(result);
        } catch (Throwable t) {
            LOGGER.error("[ThreadProxy] Error executing proxied task on main thread", t);
            pendingTask.future.completeExceptionally(t);
        }
    }

    /**
     * @return true if there are pending tasks in the queue
     */
    @ThreadSafe
    public static boolean hasPendingTasks() {
        return !TASK_QUEUE.isEmpty();
    }

    /**
     * @return the approximate number of tasks in the queue (may be inaccurate under concurrency)
     */
    @ThreadSafe
    public static int approximateQueueSize() {
        return TASK_QUEUE.size();
    }

    /**
     * @return total number of tasks proxied since server start
     */
    @ThreadSafe
    public static long getTotalProxiedCalls() {
        return TOTAL_PROXIED_CALLS.get();
    }

    /**
     * @return the peak queue size observed
     */
    @ThreadSafe
    public static long getPeakQueueSize() {
        return peakQueueSize;
    }

    /**
     * Resets monitoring metrics. Useful for periodic stat reporting.
     */
    public static void resetMetrics() {
        peakQueueSize = 0;
    }

    /**
     * Internal record for a task + its associated CompletableFuture.
     */
    private static final class PendingTask<T> {
        final Supplier<T> task;
        @SuppressWarnings("rawtypes")
        final CompletableFuture future;

        @SuppressWarnings("rawtypes")
        PendingTask(Supplier<T> task, CompletableFuture future) {
            this.task = task;
            this.future = future;
        }
    }
}

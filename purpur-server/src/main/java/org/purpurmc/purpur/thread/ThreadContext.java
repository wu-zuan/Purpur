package org.purpurmc.purpur.thread;

import org.purpurmc.purpur.thread.annotation.ThreadSafe;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Provides thread identity and delegation utilities for the Purpur multi-threading system.
 *
 * <h3>Thread model</h3>
 * <pre>
 *  ┌────────────────────────────────────────────────────────┐
 *  │  Main Thread  (server startup, shutdown, global state) │
 *  └────────────────────────────────────────────────────────┘
 *         │  dispatches world ticks each tick cycle
 *         ▼
 *  ┌───────────────────┐  ┌───────────────────┐  ┌─────────────────────┐
 *  │  WorldTicker-0    │  │  WorldTicker-1    │  │  WorldTicker-N      │
 *  │  (Overworld tick) │  │  (Nether tick)    │  │  (custom world tick)│
 *  └───────────────────┘  └───────────────────┘  └─────────────────────┘
 *         │                      │                        │
 *  (region threads: isPrimaryThread() == true during their tick window)
 *         │                      │                        │
 *         └──────────────────────┴────────────────────────┘
 *                         TaskQueue tasks flushed back to Main Thread
 * </pre>
 *
 * <h3>Plugin compatibility</h3>
 * When a plugin calls a {@link org.purpurmc.purpur.thread.annotation.MainThreadOnly} API
 * from a region thread, the vanilla guard {@code MinecraftServer.isSameThread()} returns
 * {@code true} because region threads set {@link #REGION_THREAD_FLAG} during their tick
 * window.  For API calls that genuinely cannot run off-main-thread, the plugin (or Purpur
 * compatibility shim) should call {@link #ensureMainThread(Runnable)}, which enqueues the
 * work in {@link TaskQueue} and lets the main thread drain it after all worlds finish
 * their tick.
 */
@ThreadSafe
public final class ThreadContext {

    private static final Logger LOGGER = Logger.getLogger("Purpur-ThreadContext");

    /** The true main server thread, registered exactly once at server startup. */
    private static volatile Thread mainThread;

    /**
     * Per-thread boolean that marks a thread as acting as the primary thread for a
     * specific world's tick window.  Set by {@link MultiThreadedTicker} before invoking
     * a world tick and cleared immediately after.
     */
    static final ThreadLocal<Boolean> REGION_THREAD_FLAG = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private ThreadContext() {}

    // ──────────────────────────────────────────────────────── initialisation ──

    /**
     * Registers the main server thread.  Must be called once from the main thread
     * itself, early in server startup (before any world tick begins).
     *
     * @param thread the main thread ({@code Thread.currentThread()} at call site)
     */
    public static void setMainThread(final Thread thread) {
        mainThread = thread;
        LOGGER.fine("[ThreadContext] Main thread registered: " + thread.getName());
    }

    // ────────────────────────────────────────────────────────────── queries ──

    /**
     * Returns {@code true} if the calling thread is the main server thread registered
     * via {@link #setMainThread(Thread)}.
     */
    public static boolean isMainThread() {
        return Thread.currentThread() == mainThread;
    }

    /**
     * Returns {@code true} if the calling thread is currently inside a world-tick window
     * dispatched by {@link MultiThreadedTicker} (i.e., it is a region thread).
     */
    public static boolean isRegionThread() {
        return REGION_THREAD_FLAG.get();
    }

    /**
     * Returns {@code true} if the calling thread is allowed to execute
     * {@link org.purpurmc.purpur.thread.annotation.MainThreadOnly} code — i.e., it is
     * either the real main thread <em>or</em> a region thread currently in its tick
     * window.
     *
     * <p>This is the value returned by the patched {@code MinecraftServer.isSameThread()}.
     */
    public static boolean isPrimaryThread() {
        return isMainThread() || isRegionThread();
    }

    /**
     * Returns the registered main thread, or {@code null} if
     * {@link #setMainThread(Thread)} has not been called yet.
     */
    public static Thread getMainThread() {
        return mainThread;
    }

    // ──────────────────────────────────────────────────── region-thread flag ──

    /**
     * Marks the current thread as a region thread for a world-tick window.
     * Called internally by {@link MultiThreadedTicker} before invoking a world's tick.
     * Must always be paired with a {@link #clearRegionThread()} call in a
     * {@code finally} block.
     */
    static void markAsRegionThread() {
        REGION_THREAD_FLAG.set(Boolean.TRUE);
    }

    /**
     * Clears the region-thread flag on the current thread after a tick window ends.
     */
    static void clearRegionThread() {
        REGION_THREAD_FLAG.remove();
    }

    // ───────────────────────────────────────────── main-thread delegation ──

    /**
     * Ensures {@code action} executes on the main thread.
     *
     * <ul>
     *   <li>If the caller <em>is</em> the main thread: executes immediately and returns
     *       a completed future.</li>
     *   <li>Otherwise: queues the action in {@link TaskQueue} and returns a future that
     *       completes the next time the main thread drains the queue (end of the current
     *       or next tick cycle).</li>
     * </ul>
     *
     * @param action the work to perform on the main thread
     * @param <T>    return type
     * @return a future representing the pending result
     */
    public static <T> CompletableFuture<T> ensureMainThread(final Callable<T> action) {
        if (isMainThread()) {
            try {
                return CompletableFuture.completedFuture(action.call());
            } catch (Throwable t) {
                return CompletableFuture.failedFuture(t);
            }
        }
        return TaskQueue.getInstance().submit(action);
    }

    /**
     * Void variant of {@link #ensureMainThread(Callable)}.
     *
     * @param action the work to perform on the main thread
     * @return a future that completes (with {@code null}) once the action has run
     */
    public static CompletableFuture<Void> ensureMainThread(final Runnable action) {
        return ensureMainThread(() -> {
            action.run();
            return (Void) null;
        });
    }

    /**
     * Submits {@code action} to the main thread and <strong>blocks</strong> the calling
     * thread until the result is available.
     *
     * <p><b>WARNING:</b> Never call this from the main thread itself — it will deadlock
     * because the TaskQueue is only drained after all region threads unblock.
     *
     * @param action the work to perform on the main thread
     * @param <T>    return type
     * @return the result returned by {@code action}
     * @throws ExecutionException   if {@code action} threw an exception
     * @throws InterruptedException if the current thread was interrupted while waiting
     */
    public static <T> T runOnMainThreadAndWait(final Callable<T> action)
            throws ExecutionException, InterruptedException {
        if (isMainThread()) {
            try {
                return action.call();
            } catch (Exception e) {
                throw new ExecutionException(e);
            }
        }
        return ensureMainThread(action).get();
    }
}

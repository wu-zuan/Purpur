package org.purpurmc.purpur.threading;

import org.purpurmc.purpur.threading.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Thread Context Manager — the core of the multi-threading system.
 * <p>
 * Manages per-thread state to track which region (if any) the current thread
 * is ticking, and provides utilities for determining thread identity and
 * proxying operations to the correct thread.
 * </p>
 *
 * <h3>Thread Classification:</h3>
 * <ul>
 *   <li><b>Main Thread</b> — The original server thread that runs the tick loop.
 *       Handles network I/O, plugin schedulers, and global state updates.</li>
 *   <li><b>Region Thread</b> — A worker thread from the region thread pool that
 *       ticks a specific {@link RegionContext}. Has a non-null ThreadLocal region.</li>
 *   <li><b>Async Thread</b> — Any other thread (plugin async tasks, Netty threads,
 *       chunk loading threads, etc.). Neither main nor region thread.</li>
 * </ul>
 */
public final class ThreadContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadContext.class);

    /**
     * The current region context for each thread. Null if the thread is not
     * currently ticking a region (i.e., it's the main thread or an async thread).
     */
    private static final ThreadLocal<RegionContext> CURRENT_REGION = new ThreadLocal<>();

    /**
     * Reference to the main server thread. Set once during server initialization
     * and never changed.
     */
    private static volatile Thread mainThread;

    /**
     * Whether the multi-threading system is currently active.
     */
    private static volatile boolean enabled = false;

    private ThreadContext() {} // No instantiation

    // ==================== Initialization ====================

    /**
     * Initializes the ThreadContext system. Must be called from the main server
     * thread during server startup.
     *
     * @param serverThread the main server thread
     */
    public static void initialize(Thread serverThread) {
        mainThread = serverThread;
        LOGGER.info("[ThreadContext] Initialized. Main thread: {}", serverThread.getName());
    }

    /**
     * Enables or disables the multi-threading system.
     *
     * @param enable true to enable, false to disable
     */
    public static void setEnabled(boolean enable) {
        enabled = enable;
        LOGGER.info("[ThreadContext] Multi-threading {}", enable ? "ENABLED" : "DISABLED");
    }

    /**
     * @return true if the multi-threading system is currently active
     */
    @ThreadSafe("Volatile read")
    public static boolean isEnabled() {
        return enabled;
    }

    // ==================== Thread Identity ====================

    /**
     * @return true if the current thread is the main server thread
     */
    @ThreadSafe("Volatile read + thread identity comparison")
    public static boolean isMainThread() {
        return Thread.currentThread() == mainThread;
    }

    /**
     * @return true if the current thread is a region worker thread
     *         (i.e., currently ticking a region)
     */
    @ThreadSafe("ThreadLocal read")
    public static boolean isRegionThread() {
        return CURRENT_REGION.get() != null;
    }

    /**
     * @return true if the current thread is either the main thread or a region thread.
     *         This identifies threads that are part of the tick loop and can safely
     *         access world state (with appropriate synchronization for region threads).
     */
    @ThreadSafe
    public static boolean isTickThread() {
        return isMainThread() || isRegionThread();
    }

    /**
     * @return the current region context for this thread, or null if not in a region tick
     */
    @ThreadSafe("ThreadLocal read")
    public static RegionContext getCurrentRegion() {
        return CURRENT_REGION.get();
    }

    /**
     * @return the main server thread reference
     */
    @ThreadSafe("Volatile read")
    public static Thread getMainThread() {
        return mainThread;
    }

    // ==================== Region Context Management ====================

    /**
     * Enters a region context. Called at the start of a region tick by the
     * region worker thread.
     *
     * @param region the region context to enter
     * @throws IllegalStateException if the current thread is already in a region
     */
    public static void enterRegion(RegionContext region) {
        RegionContext existing = CURRENT_REGION.get();
        if (existing != null) {
            throw new IllegalStateException(
                "Thread " + Thread.currentThread().getName() +
                " is already in region " + existing.getRegionId() +
                ", cannot enter region " + region.getRegionId()
            );
        }
        CURRENT_REGION.set(region);
        region.beginTick(Thread.currentThread());

        if (org.purpurmc.purpur.PurpurConfig.threadProxyLogging) {
            LOGGER.debug("[ThreadContext] Thread {} entered region {}",
                Thread.currentThread().getName(), region.getRegionId());
        }
    }

    /**
     * Exits the current region context. Called at the end of a region tick.
     *
     * @throws IllegalStateException if the current thread is not in any region
     */
    public static void exitRegion() {
        RegionContext region = CURRENT_REGION.get();
        if (region == null) {
            throw new IllegalStateException(
                "Thread " + Thread.currentThread().getName() +
                " is not in any region, cannot exit"
            );
        }

        region.endTick();
        CURRENT_REGION.remove();

        if (org.purpurmc.purpur.PurpurConfig.threadProxyLogging) {
            LOGGER.debug("[ThreadContext] Thread {} exited region {}",
                Thread.currentThread().getName(), region.getRegionId());
        }
    }

    // ==================== Thread Proxying Utilities ====================

    /**
     * Ensures the given operation runs on the main thread.
     * <p>
     * If already on the main thread, executes immediately.
     * If on a region thread or async thread, proxies to the main thread and waits.
     * </p>
     *
     * @param task the operation to execute on the main thread
     */
    @ThreadSafe
    public static void executeOnMainThread(Runnable task) {
        if (isMainThread() || !enabled) {
            task.run();
        } else {
            MainThreadProxy.submitAndWait(task);
        }
    }

    /**
     * Ensures the given operation runs on the main thread and returns its result.
     * <p>
     * If already on the main thread, executes immediately.
     * If on a region thread or async thread, proxies to the main thread and waits.
     * </p>
     *
     * @param task the operation to execute on the main thread
     * @param <T>  the return type
     * @return the result of the operation
     */
    @ThreadSafe
    public static <T> T executeOnMainThread(Supplier<T> task) {
        if (isMainThread() || !enabled) {
            return task.get();
        } else {
            return MainThreadProxy.submitAndWait(task);
        }
    }

    /**
     * Ensures the given operation runs on the main thread (async, no waiting).
     * <p>
     * If already on the main thread, executes immediately.
     * Otherwise, queues for later execution without blocking the caller.
     * </p>
     *
     * @param task the operation to execute on the main thread
     */
    @ThreadSafe
    public static void executeOnMainThreadAsync(Runnable task) {
        if (isMainThread() || !enabled) {
            task.run();
        } else {
            MainThreadProxy.submit(task);
        }
    }

    // ==================== Debugging ====================

    /**
     * Returns a human-readable description of the current thread's context.
     * Useful for debugging and logging.
     */
    @ThreadSafe
    public static String describeCurrentThread() {
        Thread current = Thread.currentThread();
        if (current == mainThread) {
            return "MainThread[" + current.getName() + "]";
        }
        RegionContext region = CURRENT_REGION.get();
        if (region != null) {
            return "RegionThread[" + current.getName() + ", region=" + region.getRegionId() + "]";
        }
        return "AsyncThread[" + current.getName() + "]";
    }
}

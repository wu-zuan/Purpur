package org.purpurmc.purpur.thread;

import org.purpurmc.purpur.thread.annotation.ThreadSafe;

/**
 * A daemon {@link Thread} subtype used exclusively by {@link MultiThreadedTicker}'s
 * internal thread pool.
 *
 * <h3>Identification</h3>
 * Each instance has a fixed name {@code "Purpur-WorldTicker-N"} where {@code N} is the
 * zero-based worker index assigned at creation.  This makes it easy to identify Purpur
 * workers in thread dumps and profilers.
 *
 * <h3>Region-thread flag</h3>
 * {@code RegionThread} does <em>not</em> set {@link ThreadContext#REGION_THREAD_FLAG}
 * itself.  That flag is set and cleared by {@link MultiThreadedTicker} around each
 * individual world-tick invocation so that the flag only covers the exact tick window,
 * not the entire thread lifetime.
 *
 * <h3>Usage</h3>
 * Instances are created by {@link MultiThreadedTicker}'s {@link java.util.concurrent.ThreadFactory}.
 * Direct instantiation is not needed.
 */
@ThreadSafe
public final class RegionThread extends Thread {

    /** Zero-based index assigned by the factory for logging/debug purposes. */
    private final int workerId;

    /**
     * Package-private constructor invoked only by {@link MultiThreadedTicker}'s thread
     * factory.
     *
     * @param task     the executor task runnable from {@link java.util.concurrent.ThreadPoolExecutor}
     * @param workerId zero-based worker index (used in thread name)
     */
    RegionThread(final Runnable task, final int workerId) {
        super(task, "Purpur-WorldTicker-" + workerId);
        this.workerId = workerId;
        setDaemon(true);
        setPriority(Thread.NORM_PRIORITY);
    }

    /** Returns the zero-based worker index of this thread. */
    public int getWorkerId() {
        return workerId;
    }

    /**
     * Returns {@code true} if the calling thread is a {@code RegionThread} instance
     * (regardless of whether it is currently inside a world-tick window).
     *
     * <p>To check whether the calling thread is <em>currently ticking a world</em>, use
     * {@link ThreadContext#isRegionThread()} instead.
     */
    public static boolean isCurrentThreadRegionThread() {
        return Thread.currentThread() instanceof RegionThread;
    }
}

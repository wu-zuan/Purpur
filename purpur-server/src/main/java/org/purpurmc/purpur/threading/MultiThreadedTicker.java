package org.purpurmc.purpur.threading;

import net.minecraft.server.MinecraftServer;
import net.minecraft.server.level.ServerLevel;
import org.purpurmc.purpur.PurpurConfig;
import org.purpurmc.purpur.threading.annotation.MainThreadOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Multi-Threaded Ticker — the core scheduling engine for region-based multi-threading.
 * <p>
 * This class manages a thread pool of region worker threads and orchestrates
 * the parallel ticking of worlds (regions). During a tick cycle:
 * </p>
 * <ol>
 *   <li>Each world's tick task is submitted to the thread pool</li>
 *   <li>The main thread continuously drains the {@link MainThreadProxy} queue
 *       while waiting for all world ticks to complete</li>
 *   <li>Once all world ticks finish, control returns to the main thread for
 *       post-tick processing (network, player list, GUI, etc.)</li>
 * </ol>
 *
 * <h3>Thread Pool Design:</h3>
 * <ul>
 *   <li>Fixed-size pool with daemon threads</li>
 *   <li>Thread count configurable, defaults to (CPU cores - 2, min 1)</li>
 *   <li>Named threads for easy identification in thread dumps</li>
 *   <li>Uncaught exception handler to prevent silent thread death</li>
 * </ul>
 *
 * <h3>Deadlock Prevention:</h3>
 * <p>
 * The main thread never blocks on region completion without draining the proxy queue.
 * This ensures that region threads waiting on the main thread (via MainThreadProxy)
 * are always eventually serviced, preventing circular waits.
 * </p>
 */
public class MultiThreadedTicker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiThreadedTicker.class);

    /** The region worker thread pool */
    private final ExecutorService regionPool;

    /** Number of worker threads */
    private final int threadCount;

    /** Per-world region contexts */
    private final ConcurrentHashMap<ServerLevel, RegionContext> worldRegions = new ConcurrentHashMap<>();

    /** Whether the ticker is currently active */
    private final AtomicBoolean enabled = new AtomicBoolean(true);

    /** Whether a tick cycle is currently in progress */
    private final AtomicBoolean ticking = new AtomicBoolean(false);

    /** Counter for worker thread naming */
    private static final AtomicInteger THREAD_COUNTER = new AtomicInteger(0);

    /** Monitoring: total tick cycles completed */
    private volatile long totalTickCycles = 0;

    /** Monitoring: average time spent in parallel tick (nanos) */
    private volatile long lastParallelTickNanos = 0;

    /** Monitoring: number of proxy tasks drained during last tick wait */
    private volatile int lastProxyTasksDrained = 0;

    /**
     * Creates a new MultiThreadedTicker.
     *
     * @param threadCount the number of worker threads. If <= 0, auto-detects
     *                    based on available CPU cores (cores - 2, min 1).
     */
    public MultiThreadedTicker(int threadCount) {
        if (threadCount <= 0) {
            threadCount = Math.max(1, Runtime.getRuntime().availableProcessors() - 2);
        }
        this.threadCount = threadCount;

        this.regionPool = new ThreadPoolExecutor(
            threadCount,
            threadCount,
            60L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            this::createWorkerThread,
            new ThreadPoolExecutor.CallerRunsPolicy() // Fallback: run on main thread if pool is saturated
        );

        LOGGER.info("[MultiThreadedTicker] Initialized with {} region worker threads", threadCount);
    }

    /**
     * Creates a named daemon thread for the worker pool.
     */
    private Thread createWorkerThread(Runnable runnable) {
        Thread thread = new Thread(runnable, "RegionTicker-" + THREAD_COUNTER.getAndIncrement());
        thread.setDaemon(true);
        thread.setPriority(Thread.NORM_PRIORITY);
        thread.setUncaughtExceptionHandler((t, e) -> {
            LOGGER.error("[MultiThreadedTicker] Uncaught exception in thread {}", t.getName(), e);
        });
        return thread;
    }

    /**
     * Gets or creates a RegionContext for the given world.
     *
     * @param level the server level
     * @return the region context for the world
     */
    public RegionContext getOrCreateRegion(ServerLevel level) {
        return worldRegions.computeIfAbsent(level, RegionContext::new);
    }

    /**
     * Removes the RegionContext for a world (e.g., when the world is unloaded).
     *
     * @param level the server level to remove
     */
    public void removeRegion(ServerLevel level) {
        worldRegions.remove(level);
    }

    /**
     * Ticks all worlds in parallel.
     * <p>
     * <b>MUST be called from the main thread.</b> The main thread will continuously
     * drain the {@link MainThreadProxy} queue while waiting for all region ticks
     * to complete.
     * </p>
     *
     * @param shouldKeepTicking the keep-ticking supplier (passed to each world's tick method)
     * @param worldTickAction   the action to perform for each world (encapsulates the
     *                          per-world tick setup and execution logic from the original
     *                          MinecraftServer.tickServer method)
     */
    @MainThreadOnly("Orchestrates parallel ticks from main thread")
    public void tickAllWorlds(BooleanSupplier shouldKeepTicking,
                              java.util.function.BiConsumer<ServerLevel, BooleanSupplier> worldTickAction) {

        if (!enabled.get() || ticking.getAndSet(true)) {
            LOGGER.warn("[MultiThreadedTicker] tickAllWorlds called while already ticking or disabled, falling back to sync");
            return;
        }

        long startNanos = System.nanoTime();
        int proxyDrained = 0;

        try {
            MinecraftServer server = MinecraftServer.getServer();
            List<ServerLevel> worlds = new ArrayList<>();
            for (ServerLevel level : server.getAllLevels()) {
                worlds.add(level);
            }

            if (worlds.isEmpty()) {
                return;
            }

            // If only one world, no need for threading overhead
            if (worlds.size() == 1) {
                worldTickAction.accept(worlds.get(0), shouldKeepTicking);
                return;
            }

            // Submit each world's tick to the thread pool
            List<Future<?>> futures = new ArrayList<>(worlds.size());
            for (ServerLevel level : worlds) {
                RegionContext region = getOrCreateRegion(level);
                Future<?> future = regionPool.submit(() -> {
                    ThreadContext.enterRegion(region);
                    try {
                        worldTickAction.accept(level, shouldKeepTicking);
                    } catch (Throwable t) {
                        LOGGER.error("[MultiThreadedTicker] Error ticking world {}",
                            level.dimension().location(), t);
                    } finally {
                        ThreadContext.exitRegion();
                    }
                });
                futures.add(future);
            }

            // Main thread: drain proxy queue while waiting for all worlds to finish
            boolean allDone = false;
            while (!allDone) {
                // Drain proxy tasks (execute Bukkit API calls queued by region threads)
                proxyDrained += MainThreadProxy.drainQueue(1_000_000); // 1ms budget per drain cycle

                // Check if all world ticks are complete
                allDone = true;
                for (Future<?> future : futures) {
                    if (!future.isDone()) {
                        allDone = false;
                        break;
                    }
                }

                // Brief yield to prevent busy-spinning
                if (!allDone) {
                    Thread.yield();
                }
            }

            // Final drain to catch any tasks submitted right at the end
            proxyDrained += MainThreadProxy.drainQueue(0);

            // Check for exceptions
            for (Future<?> future : futures) {
                try {
                    future.get(); // Will throw if the task threw
                } catch (ExecutionException e) {
                    LOGGER.error("[MultiThreadedTicker] World tick threw an exception", e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("[MultiThreadedTicker] Interrupted while checking world tick results", e);
                }
            }

            totalTickCycles++;

        } finally {
            ticking.set(false);
            lastParallelTickNanos = System.nanoTime() - startNanos;
            lastProxyTasksDrained = proxyDrained;
        }
    }

    /**
     * @return true if the ticker is enabled and ready to process
     */
    public boolean isEnabled() {
        return enabled.get();
    }

    /**
     * Enables or disables the ticker.
     *
     * @param enable true to enable, false to disable
     */
    public void setEnabled(boolean enable) {
        enabled.set(enable);
        ThreadContext.setEnabled(enable);
    }

    /**
     * @return true if a tick cycle is currently in progress
     */
    public boolean isTicking() {
        return ticking.get();
    }

    /**
     * Shuts down the thread pool. Called during server shutdown.
     */
    public void shutdown() {
        LOGGER.info("[MultiThreadedTicker] Shutting down region thread pool...");
        enabled.set(false);
        regionPool.shutdown();
        try {
            if (!regionPool.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.warn("[MultiThreadedTicker] Thread pool did not terminate within 10s, forcing shutdown");
                regionPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            regionPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        worldRegions.clear();
        LOGGER.info("[MultiThreadedTicker] Shutdown complete. Total tick cycles: {}", totalTickCycles);
    }

    // ==================== Monitoring ====================

    /**
     * @return the number of worker threads in the pool
     */
    public int getThreadCount() {
        return threadCount;
    }

    /**
     * @return the number of worlds/regions currently registered
     */
    public int getRegionCount() {
        return worldRegions.size();
    }

    /**
     * @return total tick cycles completed since initialization
     */
    public long getTotalTickCycles() {
        return totalTickCycles;
    }

    /**
     * @return time spent in the last parallel tick phase (nanoseconds)
     */
    public long getLastParallelTickNanos() {
        return lastParallelTickNanos;
    }

    /**
     * @return number of proxy tasks drained during the last tick wait
     */
    public int getLastProxyTasksDrained() {
        return lastProxyTasksDrained;
    }
}

package org.purpurmc.purpur.thread;

import net.minecraft.server.level.ServerLevel;
import org.purpurmc.purpur.PurpurConfig;
import org.purpurmc.purpur.thread.annotation.MainThreadOnly;
import org.purpurmc.purpur.thread.annotation.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Orchestrates parallel world-level ticking for the Purpur multi-threading system.
 *
 * <h3>Architecture</h3>
 * <pre>
 *  Main Thread (Tick N)
 *   │
 *   ├─ tickWorlds([Overworld, Nether, End, CustomWorld, …])
 *   │    │
 *   │    ├──→ WorldTicker-0: Overworld.tick()   ─┐
 *   │    ├──→ WorldTicker-1: Nether.tick()       │ parallel
 *   │    ├──→ WorldTicker-2: End.tick()           │
 *   │    └──→ WorldTicker-N: CustomWorld.tick()  ─┘
 *   │                                             │
 *   │    CompletableFuture.allOf(…).get() ◄───────┘  (main thread blocks here)
 *   │
 *   └─ TaskQueue.drainAll()   ← execute plugin API calls queued by region threads
 *
 *  Main Thread (Tick N+1) …
 * </pre>
 *
 * <h3>Plugin compatibility</h3>
 * Before invoking a world tick, each worker thread calls
 * {@link ThreadContext#markAsRegionThread()}, making
 * {@link ThreadContext#isPrimaryThread()} return {@code true}.  The patched
 * {@code MinecraftServer.isSameThread()} delegates to {@code isPrimaryThread()}, so
 * vanilla and CraftBukkit thread-safety guards do not throw while a region thread is
 * inside its tick window.
 *
 * <p>For Bukkit API calls that <em>truly</em> cannot run off the main thread (e.g.,
 * {@code Bukkit.getScheduler().runTask(...)}), region threads should enqueue work via
 * {@link ThreadContext#ensureMainThread(Runnable)}.  The main thread drains
 * {@link TaskQueue} after every {@link #tickWorlds} call.
 *
 * <h3>Configuration</h3>
 * <ul>
 *   <li>{@link PurpurConfig#multiThreadedWorldTickEnabled} — master on/off switch
 *       (default {@code false} for safety).</li>
 *   <li>{@link PurpurConfig#multiThreadedWorldTickThreadCount} — number of worker
 *       threads (default: {@code max(1, availableProcessors - 2)}).</li>
 *   <li>{@link PurpurConfig#multiThreadedWorldTickDebug} — extra logging.</li>
 *   <li>{@link PurpurConfig#taskQueueMaxDrainPerTick} — cap on tasks drained per
 *       tick (default {@link Integer#MAX_VALUE}).</li>
 * </ul>
 */
@ThreadSafe
public final class MultiThreadedTicker {

    private static final Logger LOGGER = Logger.getLogger("Purpur-MultiThreadedTicker");

    /**
     * Maximum time in seconds to wait for all world ticks to finish.
     * If exceeded, the server likely has a deadlock; we log a severe warning and
     * continue rather than hanging forever.
     */
    private static final long TICK_TIMEOUT_SECONDS = 60L;

    // ── state ─────────────────────────────────────────────────────────────────
    private final ThreadPoolExecutor executor;
    private final int threadCount;
    private final TaskQueue taskQueue;
    private volatile boolean running = true;

    // ── lifecycle ─────────────────────────────────────────────────────────────

    /**
     * Constructs the ticker, creating a fixed-size thread pool of {@link RegionThread}s.
     *
     * @param threadCount number of parallel worker threads; clamped to ≥ 1
     */
    public MultiThreadedTicker(final int threadCount) {
        this.threadCount = Math.max(1, threadCount);
        this.taskQueue = TaskQueue.getInstance();

        final AtomicInteger workerIndex = new AtomicInteger(0);
        this.executor = new ThreadPoolExecutor(
                this.threadCount,
                this.threadCount,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                // Custom factory produces RegionThread instances
                runnable -> new RegionThread(runnable, workerIndex.getAndIncrement())
        );

        // Pre-start all core threads so they are warm for the first tick
        this.executor.prestartAllCoreThreads();

        LOGGER.info("[MultiThreadedTicker] Initialized with " + this.threadCount
                + " world-tick worker thread(s).");
    }

    // ── tick ──────────────────────────────────────────────────────────────────

    /**
     * Ticks all provided worlds in parallel, then drains the main-thread task queue.
     *
     * <p>This method <strong>blocks</strong> the calling (main) thread until every world
     * tick completes (or the {@value #TICK_TIMEOUT_SECONDS}-second watchdog fires).
     * After all worlds have finished, {@link TaskQueue} is drained synchronously on the
     * main thread before this method returns.
     *
     * <p>When multi-threaded ticking is disabled via configuration, or when only one
     * world is loaded, worlds are ticked sequentially on the main thread with no
     * overhead.
     *
     * @param worlds list of worlds to tick this cycle; must not be {@code null}
     * @param tickFn the tick function to invoke for each world (e.g., {@code level::tick})
     */
    @MainThreadOnly
    public void tickWorlds(final List<ServerLevel> worlds, final Consumer<ServerLevel> tickFn) {
        if (!running || worlds.isEmpty()) return;

        // ── fast path: single world or feature disabled ──────────────────────
        if (worlds.size() == 1 || !PurpurConfig.multiThreadedWorldTickEnabled) {
            for (final ServerLevel world : worlds) {
                tickFn.accept(world);
            }
            taskQueue.drain(PurpurConfig.taskQueueMaxDrainPerTick);
            return;
        }

        // ── parallel path ────────────────────────────────────────────────────
        if (PurpurConfig.multiThreadedWorldTickDebug) {
            LOGGER.fine("[MultiThreadedTicker] Dispatching " + worlds.size()
                    + " world(s) to " + threadCount + " thread(s).");
        }

        final List<CompletableFuture<Void>> futures = new ArrayList<>(worlds.size());
        for (final ServerLevel world : worlds) {
            futures.add(CompletableFuture.runAsync(
                    () -> tickWorldOnRegionThread(world, tickFn),
                    executor
            ));
        }

        // Block main thread until all world ticks complete (with timeout guard)
        final CompletableFuture<Void> all =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            all.get(TICK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (final TimeoutException e) {
            LOGGER.severe("[MultiThreadedTicker] World ticks did not complete within "
                    + TICK_TIMEOUT_SECONDS + "s! The server may be deadlocked. "
                    + "Attempting to continue — check region lock usage.");
            // Cancel futures that are still running to avoid perpetual hang
            futures.forEach(f -> f.cancel(true));
        } catch (final ExecutionException e) {
            LOGGER.log(Level.SEVERE,
                    "[MultiThreadedTicker] Uncaught exception from a world-tick future",
                    e.getCause());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ── drain plugin API tasks queued by region threads ───────────────────
        final int drained = taskQueue.drain(PurpurConfig.taskQueueMaxDrainPerTick);
        if (drained > 0 && PurpurConfig.multiThreadedWorldTickDebug) {
            LOGGER.fine("[MultiThreadedTicker] Drained " + drained
                    + " main-thread task(s) after tick.");
        }
    }

    /**
     * Executes a single world tick on the calling region thread, sandwiched by
     * {@link ThreadContext#markAsRegionThread()} / {@link ThreadContext#clearRegionThread()}
     * so that vanilla thread guards pass for the duration of the tick.
     */
    private void tickWorldOnRegionThread(final ServerLevel world,
                                         final Consumer<ServerLevel> tickFn) {
        ThreadContext.markAsRegionThread();
        try {
            tickFn.accept(world);
        } catch (final Throwable t) {
            final String worldName = world.dimension().identifier().toString();
            LOGGER.log(Level.SEVERE,
                    "[MultiThreadedTicker] Unhandled exception while ticking world '"
                            + worldName + "'", t);
        } finally {
            ThreadContext.clearRegionThread();
        }
    }

    // ── shutdown ──────────────────────────────────────────────────────────────

    /**
     * Signals the ticker to stop and shuts down the thread pool, waiting up to 10
     * seconds for any in-flight ticks to complete.
     *
     * <p>After {@code shutdown()} returns, {@link #tickWorlds} is a no-op.
     */
    public void shutdown() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10L, TimeUnit.SECONDS)) {
                LOGGER.warning("[MultiThreadedTicker] Thread pool did not terminate within 10 s;"
                        + " forcing shutdown.");
                executor.shutdownNow();
            }
        } catch (final InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOGGER.info("[MultiThreadedTicker] Shut down cleanly.");
    }

    // ── query ─────────────────────────────────────────────────────────────────

    /** Returns the number of worker threads in this ticker's thread pool. */
    public int getThreadCount() {
        return threadCount;
    }

    /** Returns {@code true} while the ticker has not been {@link #shutdown()}. */
    public boolean isRunning() {
        return running;
    }

    /**
     * Returns a human-readable status string suitable for the {@code /purpur} command or
     * server-status output.
     */
    public String getStatusString() {
        return String.format("MultiThreadedTicker[threads=%d, running=%b, "
                        + "taskQueue.pending=%d, taskQueue.totalDrained=%d]",
                threadCount, running,
                taskQueue.pendingCount(),
                taskQueue.totalDrained());
    }
}

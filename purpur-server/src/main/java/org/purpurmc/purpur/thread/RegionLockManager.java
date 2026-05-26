package org.purpurmc.purpur.thread;

import org.purpurmc.purpur.thread.annotation.ThreadSafe;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Manages a pool of {@link ReentrantLock} instances keyed by {@link RegionKey}, providing
 * deadlock-safe locking for cross-region and cross-world interactions.
 *
 * <h3>When to use</h3>
 * <ul>
 *   <li>Entity movement across a region boundary (two adjacent regions need to be locked
 *       simultaneously to move the entity atomically).</li>
 *   <li>Cross-world teleportation (lock the source region in World A and the destination
 *       region in World B).</li>
 *   <li>Redstone pulse propagation that crosses a chunk-region boundary.</li>
 *   <li>Any read-modify-write operation on data shared between two parallel world ticks.</li>
 * </ul>
 *
 * <h3>Deadlock prevention</h3>
 * {@link #withLocks(RegionKey[], Runnable)} sorts the provided keys via their natural
 * {@link Comparable} order before acquiring locks, guaranteeing that any two concurrent
 * callers always acquire the same set of locks in the same global order — the
 * classic <em>lock ordering</em> strategy.
 *
 * <h3>Fair locks</h3>
 * All {@link ReentrantLock} instances are created with {@code fair = true} to prevent
 * thread starvation when many region threads compete for the same border lock.
 *
 * <h3>Contention guidance</h3>
 * Keep critical sections as small as possible.  Avoid holding a region lock while
 * performing I/O or calling into unknown plugin code.  If you only need to protect a
 * brief read or write, prefer the {@link #tryWithLock} variant which times out after
 * {@value #LOCK_TIMEOUT_MS} ms rather than blocking indefinitely.
 */
@ThreadSafe
public final class RegionLockManager {

    private static final Logger LOGGER = Logger.getLogger("Purpur-RegionLockManager");

    /** Max time (ms) to wait for a lock before {@link #tryWithLock} gives up. */
    private static final long LOCK_TIMEOUT_MS = 200L;

    /** Singleton. */
    private static final RegionLockManager INSTANCE = new RegionLockManager();

    /**
     * Lock map.  Entries are created on first access and removed lazily during
     * {@link #cleanup()} to bound memory growth on servers with large worlds.
     */
    private final ConcurrentHashMap<RegionKey, ReentrantLock> locks = new ConcurrentHashMap<>();

    private RegionLockManager() {}

    /** Returns the singleton {@code RegionLockManager}. */
    public static RegionLockManager getInstance() {
        return INSTANCE;
    }

    // ─────────────────────────────────────────────────── single-key API ──

    /**
     * Acquires the lock for {@code key}, runs {@code action}, then releases the lock.
     *
     * @param key    the region to lock
     * @param action the work to execute while the lock is held
     */
    public void withLock(final RegionKey key, final Runnable action) {
        final ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Acquiring variant that returns a value from the critical section.
     *
     * @param key      the region to lock
     * @param supplier the work to execute while the lock is held
     * @param <T>      the return type
     * @return the value returned by {@code supplier}
     */
    public <T> T withLock(final RegionKey key, final Supplier<T> supplier) {
        final ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            return supplier.get();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Attempts to acquire the lock for {@code key} within {@value #LOCK_TIMEOUT_MS} ms.
     * If the lock cannot be obtained in time, logs a warning and returns {@code false}
     * without executing {@code action}.
     *
     * <p>This is the preferred variant for hot-path code where a brief retry-later
     * strategy (e.g., deferring entity movement to the next tick) is better than
     * blocking the region thread and reducing TPS.
     *
     * @param key    the region to lock
     * @param action the work to execute if the lock is acquired
     * @return {@code true} if the action was executed; {@code false} on timeout
     */
    public boolean tryWithLock(final RegionKey key, final Runnable action) {
        final ReentrantLock lock = getLock(key);
        try {
            if (!lock.tryLock(LOCK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                LOGGER.warning("[RegionLockManager] Lock timeout (" + LOCK_TIMEOUT_MS
                        + " ms) for " + key + " — action skipped to preserve TPS.");
                return false;
            }
            try {
                action.run();
                return true;
            } finally {
                lock.unlock();
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ─────────────────────────────────────────────────── multi-key API ──

    /**
     * Acquires locks for all provided {@code keys} in a consistent sorted order, executes
     * {@code action}, then releases all locks in reverse order.
     *
     * <p>Duplicate and {@code null} entries in {@code keys} are silently ignored.
     * The sort order is determined by {@link RegionKey#compareTo(RegionKey)}, which is
     * stable across JVM invocations.
     *
     * <h3>Example — cross-region entity move</h3>
     * <pre>{@code
     *   RegionKey src  = RegionKey.of(level, entity.getBlockX(), entity.getBlockZ());
     *   RegionKey dest = RegionKey.of(level, targetX, targetZ);
     *   RegionLockManager.getInstance().withLocks(new RegionKey[]{src, dest}, () -> {
     *       level.removeEntity(entity);
     *       entity.setPos(targetX, targetY, targetZ);
     *       level.addEntity(entity);
     *   });
     * }</pre>
     *
     * @param keys   the regions to lock; must not be {@code null}
     * @param action the work to perform while all locks are held
     */
    public void withLocks(final RegionKey[] keys, final Runnable action) {
        // Deduplicate, filter nulls, sort for consistent ordering
        final RegionKey[] sorted = Arrays.stream(keys)
                .filter(k -> k != null)
                .distinct()
                .sorted()
                .toArray(RegionKey[]::new);

        acquireAll(sorted, 0, action);
    }

    /**
     * Recursive helper: acquires {@code sorted[index]}, then recurses to acquire the
     * next key, executes the action at the leaf, and unwinds releases in reverse.
     */
    private void acquireAll(final RegionKey[] sorted, final int index, final Runnable action) {
        if (index == sorted.length) {
            action.run();
            return;
        }
        final ReentrantLock lock = getLock(sorted[index]);
        lock.lock();
        try {
            acquireAll(sorted, index + 1, action);
        } finally {
            lock.unlock();
        }
    }

    // ──────────────────────────────────────────────── lock access ──

    /**
     * Returns (creating if absent) the fair {@link ReentrantLock} for the given key.
     *
     * @param key the region key
     * @return the lock, never {@code null}
     */
    public ReentrantLock getLock(final RegionKey key) {
        return locks.computeIfAbsent(key, k -> new ReentrantLock(true /* fair */));
    }

    // ────────────────────────────────────────────── maintenance ──

    /**
     * Removes all lock entries that are currently unheld and have no waiting threads.
     * Should be called periodically (e.g., once per minute) to prevent unbounded memory
     * growth on servers with large, sparsely-explored worlds where many unique region
     * keys are created but seldom reused.
     *
     * @return the number of lock entries removed
     */
    public int cleanup() {
        int removed = 0;
        final Iterator<Map.Entry<RegionKey, ReentrantLock>> it = locks.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<RegionKey, ReentrantLock> entry = it.next();
            final ReentrantLock lock = entry.getValue();
            // Only remove if nobody holds it and no thread is queued for it
            if (!lock.isLocked() && lock.getQueueLength() == 0) {
                it.remove();
                removed++;
            }
        }
        if (removed > 0) {
            LOGGER.fine("[RegionLockManager] Cleaned up " + removed + " stale region lock(s). "
                    + "Active locks: " + locks.size());
        }
        return removed;
    }

    /** Returns the number of region locks currently tracked. */
    public int lockCount() {
        return locks.size();
    }
}

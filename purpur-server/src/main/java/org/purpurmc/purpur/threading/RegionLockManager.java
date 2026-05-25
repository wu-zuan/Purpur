package org.purpurmc.purpur.threading;

import net.minecraft.world.level.ChunkPos;
import org.purpurmc.purpur.threading.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * Region Lock Manager — provides fine-grained locking for cross-region interactions.
 * <p>
 * Uses {@link StampedLock} for chunk-level locking, which provides:
 * <ul>
 *   <li><b>Optimistic reads</b> — Zero-overhead reads when no writer is active (99% case)</li>
 *   <li><b>Read locks</b> — Fallback when optimistic read detects a concurrent write</li>
 *   <li><b>Write locks</b> — Exclusive access for modifications</li>
 * </ul>
 * </p>
 *
 * <h3>Lock Ordering for Deadlock Prevention:</h3>
 * <p>
 * When acquiring locks on multiple chunks (e.g., cross-region entity movement),
 * locks are always acquired in a deterministic order based on the chunk's
 * packed coordinate ({@link ChunkPos#toLong(int, int)}). This prevents
 * ABBA-style deadlocks.
 * </p>
 *
 * <h3>Lock Strategy by Operation Type:</h3>
 * <table>
 *   <tr><th>Operation</th><th>Lock Type</th><th>Reason</th></tr>
 *   <tr><td>Block read</td><td>Optimistic read</td><td>99% no contention</td></tr>
 *   <tr><td>Block write</td><td>Write lock</td><td>Data consistency</td></tr>
 *   <tr><td>Entity move (same region)</td><td>None</td><td>Single-thread ownership</td></tr>
 *   <tr><td>Entity move (cross region)</td><td>Dual write lock</td><td>Both regions affected</td></tr>
 *   <tr><td>Redstone cross-region</td><td>Main thread proxy</td><td>Too complex for fine locks</td></tr>
 * </table>
 */
@ThreadSafe
public class RegionLockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegionLockManager.class);

    /**
     * Per-chunk StampedLocks, lazily created.
     * Key: ChunkPos.toLong() (packed x/z coordinate).
     */
    private final ConcurrentHashMap<Long, StampedLock> chunkLocks = new ConcurrentHashMap<>();

    /** Singleton instance */
    private static final RegionLockManager INSTANCE = new RegionLockManager();

    private RegionLockManager() {}

    /**
     * @return the singleton RegionLockManager instance
     */
    public static RegionLockManager getInstance() {
        return INSTANCE;
    }

    // ==================== Lock Acquisition ====================

    /**
     * Gets or creates a StampedLock for the given chunk.
     *
     * @param chunkKey the packed chunk coordinate (ChunkPos.toLong())
     * @return the StampedLock for the chunk
     */
    @ThreadSafe("ConcurrentHashMap.computeIfAbsent is thread-safe")
    private StampedLock getLock(long chunkKey) {
        return chunkLocks.computeIfAbsent(chunkKey, k -> new StampedLock());
    }

    /**
     * Attempts an optimistic read on the given chunk.
     * <p>
     * This is the cheapest locking operation — it doesn't actually acquire a lock,
     * just reads a version stamp. After performing the read, the caller must
     * validate the stamp using {@link #validateOptimisticRead(long, long)}.
     * </p>
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @return the optimistic read stamp (pass to validateOptimisticRead)
     */
    public long tryOptimisticRead(int chunkX, int chunkZ) {
        long key = ChunkPos.asLong(chunkX, chunkZ);
        return getLock(key).tryOptimisticRead();
    }

    /**
     * Validates an optimistic read stamp.
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @param stamp  the stamp returned by tryOptimisticRead
     * @return true if no writes occurred since the stamp was obtained
     */
    public boolean validateOptimisticRead(int chunkX, int chunkZ, long stamp) {
        long key = ChunkPos.asLong(chunkX, chunkZ);
        return getLock(key).validate(stamp);
    }

    /**
     * Acquires a read lock on the given chunk. Blocks until available.
     * <p>
     * Use this as a fallback when an optimistic read fails (i.e., a concurrent
     * write was detected). The returned stamp must be passed to
     * {@link #releaseReadLock(int, int, long)}.
     * </p>
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @return the read lock stamp
     */
    public long acquireReadLock(int chunkX, int chunkZ) {
        long key = ChunkPos.asLong(chunkX, chunkZ);
        return getLock(key).readLock();
    }

    /**
     * Releases a read lock on the given chunk.
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @param stamp  the stamp returned by acquireReadLock
     */
    public void releaseReadLock(int chunkX, int chunkZ, long stamp) {
        long key = ChunkPos.asLong(chunkX, chunkZ);
        getLock(key).unlockRead(stamp);
    }

    /**
     * Acquires a write lock on the given chunk. Blocks until available.
     * <p>
     * The returned stamp must be passed to {@link #releaseWriteLock(int, int, long)}.
     * </p>
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @return the write lock stamp
     */
    public long acquireWriteLock(int chunkX, int chunkZ) {
        long key = ChunkPos.asLong(chunkX, chunkZ);
        return getLock(key).writeLock();
    }

    /**
     * Releases a write lock on the given chunk.
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @param stamp  the stamp returned by acquireWriteLock
     */
    public void releaseWriteLock(int chunkX, int chunkZ, long stamp) {
        long key = ChunkPos.asLong(chunkX, chunkZ);
        getLock(key).unlockWrite(stamp);
    }

    // ==================== Cross-Region Operations ====================

    /**
     * Executes an action while holding write locks on two chunks.
     * <p>
     * Locks are acquired in a deterministic order (by packed coordinate value)
     * to prevent deadlocks. This is used for operations that span two regions,
     * such as entity movement across region boundaries.
     * </p>
     *
     * @param sourceX source chunk X
     * @param sourceZ source chunk Z
     * @param targetX target chunk X
     * @param targetZ target chunk Z
     * @param action  the action to execute while holding both locks
     */
    public void executeWithCrossRegionLock(
            int sourceX, int sourceZ,
            int targetX, int targetZ,
            Runnable action) {

        long sourceKey = ChunkPos.asLong(sourceX, sourceZ);
        long targetKey = ChunkPos.asLong(targetX, targetZ);

        // Same chunk — only need one lock
        if (sourceKey == targetKey) {
            long stamp = acquireWriteLock(sourceX, sourceZ);
            try {
                action.run();
            } finally {
                releaseWriteLock(sourceX, sourceZ, stamp);
            }
            return;
        }

        // Acquire locks in deterministic order to prevent deadlocks
        long firstKey, secondKey;
        int firstX, firstZ, secondX, secondZ;
        if (sourceKey < targetKey) {
            firstKey = sourceKey; firstX = sourceX; firstZ = sourceZ;
            secondKey = targetKey; secondX = targetX; secondZ = targetZ;
        } else {
            firstKey = targetKey; firstX = targetX; firstZ = targetZ;
            secondKey = sourceKey; secondX = sourceX; secondZ = sourceZ;
        }

        long stamp1 = acquireWriteLock(firstX, firstZ);
        try {
            long stamp2 = acquireWriteLock(secondX, secondZ);
            try {
                action.run();
            } finally {
                releaseWriteLock(secondX, secondZ, stamp2);
            }
        } finally {
            releaseWriteLock(firstX, firstZ, stamp1);
        }
    }

    // ==================== Convenience: Optimistic-Read-Then-Retry ====================

    /**
     * Performs a read operation with optimistic locking, falling back to a
     * pessimistic read lock if the optimistic read fails.
     * <p>
     * This is the recommended pattern for reading chunk data:
     * </p>
     * <pre>{@code
     * BlockState state = RegionLockManager.getInstance().readWithOptimisticLock(
     *     chunkX, chunkZ,
     *     () -> chunk.getBlockState(pos)
     * );
     * }</pre>
     *
     * @param chunkX the chunk X coordinate
     * @param chunkZ the chunk Z coordinate
     * @param reader the read operation to perform
     * @param <T>    the return type
     * @return the result of the read operation
     */
    public <T> T readWithOptimisticLock(int chunkX, int chunkZ, java.util.function.Supplier<T> reader) {
        // First: try optimistic read (zero overhead)
        long stamp = tryOptimisticRead(chunkX, chunkZ);
        T result = reader.get();
        if (validateOptimisticRead(chunkX, chunkZ, stamp)) {
            return result; // No concurrent write — fast path!
        }

        // Fallback: pessimistic read lock
        stamp = acquireReadLock(chunkX, chunkZ);
        try {
            return reader.get();
        } finally {
            releaseReadLock(chunkX, chunkZ, stamp);
        }
    }

    // ==================== Cleanup ====================

    /**
     * Removes locks for chunks that are no longer loaded.
     * Should be called periodically (e.g., every few minutes) to prevent memory leaks.
     *
     * @param isChunkLoaded predicate to check if a chunk (by packed coordinate) is still loaded
     */
    public void cleanupUnloadedChunks(java.util.function.LongPredicate isChunkLoaded) {
        chunkLocks.keySet().removeIf(key -> !isChunkLoaded.test(key));
    }

    /**
     * @return the number of chunk locks currently tracked
     */
    public int getTrackedChunkCount() {
        return chunkLocks.size();
    }
}

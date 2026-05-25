package org.purpurmc.purpur.threading;

import net.minecraft.server.level.ServerLevel;
import net.minecraft.world.level.ChunkPos;
import org.purpurmc.purpur.threading.annotation.ThreadSafe;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents the execution context for a single region.
 * <p>
 * In Phase 1, each {@link ServerLevel} (world) maps to exactly one Region.
 * Future phases may subdivide worlds into multiple regions based on player
 * distribution for finer-grained parallelism.
 * </p>
 * <p>
 * Each RegionContext is bound to exactly one worker thread at any given time
 * during a tick. The owning thread is set when the region tick begins and
 * cleared when it ends.
 * </p>
 */
public class RegionContext {

    private final ServerLevel level;
    private final String regionId;
    private final Set<ChunkPos> ownedChunks;
    private final ReentrantLock regionLock;
    private final ConcurrentLinkedQueue<Runnable> pendingTasks;

    private volatile Thread ownerThread;
    private volatile boolean ticking;

    /**
     * Creates a new RegionContext for the given world.
     *
     * @param level the server level this region belongs to
     */
    public RegionContext(ServerLevel level) {
        this.level = level;
        this.regionId = level.dimension().location().toString();
        this.ownedChunks = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.regionLock = new ReentrantLock(true); // Fair lock to prevent starvation
        this.pendingTasks = new ConcurrentLinkedQueue<>();
    }

    /**
     * Marks this region as currently being ticked by the given thread.
     * Called at the start of a region tick.
     *
     * @param thread the thread that will tick this region
     */
    public void beginTick(Thread thread) {
        this.ownerThread = thread;
        this.ticking = true;
    }

    /**
     * Marks this region as no longer being ticked.
     * Processes any remaining pending tasks before clearing.
     * Called at the end of a region tick.
     */
    public void endTick() {
        // Drain any remaining pending tasks
        drainPendingTasks();
        this.ticking = false;
        this.ownerThread = null;
    }

    /**
     * Drains and executes all pending tasks queued for this region.
     * Should only be called from the region's owner thread.
     */
    public void drainPendingTasks() {
        Runnable task;
        while ((task = pendingTasks.poll()) != null) {
            try {
                task.run();
            } catch (Exception e) {
                org.slf4j.LoggerFactory.getLogger(RegionContext.class)
                    .error("[Region {}] Error executing pending task", regionId, e);
            }
        }
    }

    /**
     * Queues a task to be executed on this region's thread during the next drain cycle.
     *
     * @param task the task to queue
     */
    public void queueTask(Runnable task) {
        pendingTasks.add(task);
    }

    /**
     * Checks if the given chunk position is owned by this region.
     *
     * @param pos the chunk position to check
     * @return true if this region owns the chunk
     */
    @ThreadSafe("ConcurrentHashMap-backed set")
    public boolean ownsChunk(ChunkPos pos) {
        // In Phase 1 (per-world regions), all chunks in the world belong to this region
        return true;
    }

    /**
     * Checks if the given block position is owned by this region.
     *
     * @param x block x coordinate
     * @param z block z coordinate
     * @return true if this region owns the block position
     */
    @ThreadSafe
    public boolean ownsPosition(int x, int z) {
        // In Phase 1, all positions in the world belong to this region
        return true;
    }

    /**
     * @return the server level associated with this region
     */
    @ThreadSafe("Immutable reference")
    public ServerLevel getLevel() {
        return level;
    }

    /**
     * @return a unique identifier for this region
     */
    @ThreadSafe("Immutable reference")
    public String getRegionId() {
        return regionId;
    }

    /**
     * @return the lock for this region, used for cross-region synchronization
     */
    @ThreadSafe("Immutable reference")
    public ReentrantLock getRegionLock() {
        return regionLock;
    }

    /**
     * @return the thread currently owning (ticking) this region, or null
     */
    @ThreadSafe("Volatile read")
    public Thread getOwnerThread() {
        return ownerThread;
    }

    /**
     * @return true if this region is currently being ticked
     */
    @ThreadSafe("Volatile read")
    public boolean isTicking() {
        return ticking;
    }

    /**
     * @return true if the current thread is the owner of this region
     */
    @ThreadSafe
    public boolean isOwnedByCurrentThread() {
        return Thread.currentThread() == ownerThread;
    }

    @Override
    public String toString() {
        return "RegionContext{" +
            "regionId='" + regionId + '\'' +
            ", ticking=" + ticking +
            ", owner=" + (ownerThread != null ? ownerThread.getName() : "none") +
            '}';
    }
}

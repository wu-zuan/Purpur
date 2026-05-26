package org.purpurmc.purpur.thread;

import net.minecraft.server.level.ServerLevel;
import org.purpurmc.purpur.thread.annotation.ThreadSafe;

/**
 * Immutable key that uniquely identifies a rectangular 512 × 512-block region inside a
 * specific Minecraft dimension.
 *
 * <h3>Region sizing</h3>
 * Each region spans {@value #REGION_SIZE_BLOCKS} blocks on both axes (32 chunks × 16
 * blocks/chunk).  Block coordinates are mapped to region coordinates via integer floor
 * division:
 * <pre>
 *   regionX = Math.floorDiv(blockX, REGION_SIZE_BLOCKS)
 *   regionZ = Math.floorDiv(blockZ, REGION_SIZE_BLOCKS)
 * </pre>
 *
 * <h3>Usage in {@link RegionLockManager}</h3>
 * {@code RegionKey} implements {@link Comparable} so that
 * {@link RegionLockManager#withLocks(RegionKey[], Runnable)} can sort keys before
 * acquiring locks, guaranteeing a consistent global lock ordering and preventing
 * deadlocks when two threads each need locks on adjacent regions.
 *
 * <h3>Cross-dimension locks</h3>
 * {@link #ofDimension(ServerLevel)} creates a sentinel key (with {@code regionX} and
 * {@code regionZ} set to {@link Integer#MIN_VALUE}) that represents an entire dimension.
 * Use this when an operation must hold exclusive access to the whole world (e.g.,
 * cross-dimension entity teleportation).
 */
@ThreadSafe
public record RegionKey(String dimension, int regionX, int regionZ) implements Comparable<RegionKey> {

    /** Number of blocks per region axis (32 chunks × 16 blocks = 512 blocks). */
    public static final int REGION_SIZE_BLOCKS = 512;

    // ──────────────────────────────────────────────────────────── factories ──

    /**
     * Creates a {@code RegionKey} from world-space block coordinates.
     *
     * @param level  the world containing the coordinates
     * @param blockX block X coordinate (may be negative)
     * @param blockZ block Z coordinate (may be negative)
     * @return the region key covering those coordinates
     */
    public static RegionKey of(final ServerLevel level, final int blockX, final int blockZ) {
        return new RegionKey(
                level.dimension().identifier().toString(),
                Math.floorDiv(blockX, REGION_SIZE_BLOCKS),
                Math.floorDiv(blockZ, REGION_SIZE_BLOCKS)
        );
    }

    /**
     * Creates a dimension-wide {@code RegionKey}.  Acquiring this key through
     * {@link RegionLockManager} gives exclusive access to the entire dimension, useful
     * for operations that span the whole world.
     *
     * @param level the world to create a dimension-wide key for
     * @return a sentinel key representing the whole dimension
     */
    public static RegionKey ofDimension(final ServerLevel level) {
        return new RegionKey(
                level.dimension().identifier().toString(),
                Integer.MIN_VALUE,
                Integer.MIN_VALUE
        );
    }

    // ──────────────────────────────────────────────────────────── queries ──

    /**
     * Returns {@code true} if this key was created via {@link #ofDimension(ServerLevel)}
     * (i.e., it represents an entire dimension rather than a specific region).
     */
    public boolean isDimensionWide() {
        return regionX == Integer.MIN_VALUE && regionZ == Integer.MIN_VALUE;
    }

    // ──────────────────────────────────────────────────────── ordering ──

    /**
     * Consistent total order used by {@link RegionLockManager} to sort lock acquisition
     * sequence and prevent deadlocks.
     *
     * <p>Ordering: dimension name (lexicographic) → regionX → regionZ.
     */
    @Override
    public int compareTo(final RegionKey other) {
        int cmp = this.dimension.compareTo(other.dimension);
        if (cmp != 0) return cmp;
        cmp = Integer.compare(this.regionX, other.regionX);
        if (cmp != 0) return cmp;
        return Integer.compare(this.regionZ, other.regionZ);
    }

    @Override
    public String toString() {
        return isDimensionWide()
                ? "RegionKey[dim=" + dimension + ", DIMENSION_WIDE]"
                : "RegionKey[dim=" + dimension + ", rx=" + regionX + ", rz=" + regionZ + "]";
    }
}

package org.purpurmc.purpur.threading;

import org.purpurmc.purpur.PurpurConfig;
import org.purpurmc.purpur.threading.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Thread-Safe Bukkit API wrapper — the plugin compatibility layer.
 * <p>
 * This class provides utility methods to ensure Bukkit API operations are
 * executed on the correct thread. When a plugin calls a Bukkit API method
 * from a region worker thread (which is not the main thread), this class
 * automatically proxies the call back to the main thread via
 * {@link MainThreadProxy}.
 * </p>
 *
 * <h3>How it works:</h3>
 * <pre>{@code
 * // In CraftWorld.setBlockData():
 * ThreadSafeBukkitAPI.ensureMainThread("World.setBlockData", () -> {
 *     // Original block-setting logic here
 *     this.handle.setBlock(pos, blockData, flags);
 * });
 * }</pre>
 *
 * <h3>When multi-threading is DISABLED:</h3>
 * <p>
 * All methods in this class execute the action directly without any overhead.
 * The system is completely transparent when multi-threading is not active.
 * </p>
 *
 * <h3>Performance characteristics:</h3>
 * <ul>
 *   <li>On main thread: 1 volatile read + direct execution (near-zero overhead)</li>
 *   <li>On region thread: CompletableFuture submission + blocking wait</li>
 *   <li>When disabled: direct execution (zero overhead)</li>
 * </ul>
 */
@ThreadSafe
public final class ThreadSafeBukkitAPI {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadSafeBukkitAPI.class);

    private ThreadSafeBukkitAPI() {} // No instantiation

    /**
     * Ensures the given action runs on the main thread.
     * <p>
     * If multi-threading is disabled OR the current thread is already the main
     * thread, the action is executed directly. Otherwise, the action is proxied
     * to the main thread via {@link MainThreadProxy} and the calling thread
     * blocks until completion.
     * </p>
     *
     * @param apiName a human-readable name of the API being called (for logging)
     * @param action  the action to execute on the main thread
     */
    public static void ensureMainThread(String apiName, Runnable action) {
        if (!ThreadContext.isEnabled() || ThreadContext.isMainThread()) {
            action.run();
            return;
        }

        logProxy(apiName);
        MainThreadProxy.submitAndWait(action);
    }

    /**
     * Ensures the given action runs on the main thread, returning a result.
     * <p>
     * If multi-threading is disabled OR the current thread is already the main
     * thread, the action is executed directly and the result returned.
     * Otherwise, the action is proxied to the main thread and the calling
     * thread blocks until the result is available.
     * </p>
     *
     * @param apiName a human-readable name of the API being called (for logging)
     * @param action  the action to execute on the main thread
     * @param <T>     the return type
     * @return the result of the action
     */
    public static <T> T ensureMainThread(String apiName, Supplier<T> action) {
        if (!ThreadContext.isEnabled() || ThreadContext.isMainThread()) {
            return action.get();
        }

        logProxy(apiName);
        return MainThreadProxy.submitAndWait(action);
    }

    /**
     * Ensures the given action runs on the main thread (fire-and-forget).
     * <p>
     * Unlike {@link #ensureMainThread(String, Runnable)}, this method does NOT
     * block the calling thread. Use this for operations where the caller doesn't
     * need to wait for the result (e.g., sending chat messages, firing events
     * where the return value is not critical).
     * </p>
     *
     * @param apiName a human-readable name of the API being called (for logging)
     * @param action  the action to execute on the main thread
     */
    public static void ensureMainThreadAsync(String apiName, Runnable action) {
        if (!ThreadContext.isEnabled() || ThreadContext.isMainThread()) {
            action.run();
            return;
        }

        logProxy(apiName);
        MainThreadProxy.submit(action);
    }

    /**
     * Checks if an operation needs to be proxied to the main thread.
     * <p>
     * This is a lightweight check that can be used in hot paths to decide
     * whether to call the proxy method or execute directly.
     * </p>
     *
     * @return true if the current thread is NOT the main thread AND
     *         multi-threading is enabled
     */
    public static boolean needsProxy() {
        return ThreadContext.isEnabled() && !ThreadContext.isMainThread();
    }

    /**
     * Validates that the current thread is the main thread or a tick thread.
     * <p>
     * This is similar to Bukkit's existing isPrimaryThread() check, but
     * extended to recognize region worker threads as valid tick threads.
     * </p>
     * <p>
     * Use this for operations that need to be on a tick thread but don't
     * necessarily need to be on the main thread specifically.
     * </p>
     *
     * @param apiName the API name (for error messages)
     * @throws IllegalStateException if the current thread is not a tick thread
     */
    public static void requireTickThread(String apiName) {
        if (!ThreadContext.isTickThread()) {
            throw new IllegalStateException(
                apiName + " must be called from a tick thread (main or region), " +
                "but was called from: " + ThreadContext.describeCurrentThread()
            );
        }
    }

    /**
     * Enhanced version of Bukkit's isPrimaryThread() check that accounts for
     * region worker threads.
     * <p>
     * In the multi-threading model, region worker threads are considered
     * "primary" threads for their respective region. This method returns
     * true for both the main thread and region worker threads.
     * </p>
     *
     * @return true if the current thread is either the main thread or a
     *         region worker thread
     */
    public static boolean isPrimaryThread() {
        if (!ThreadContext.isEnabled()) {
            // When multi-threading is disabled, fall back to traditional check
            return ThreadContext.isMainThread();
        }
        // When enabled, both main thread and region threads are "primary"
        return ThreadContext.isTickThread();
    }

    /**
     * Logs a proxy event if proxy logging is enabled in configuration.
     */
    private static void logProxy(String apiName) {
        if (PurpurConfig.threadProxyLogging) {
            LOGGER.info("[ThreadProxy] Proxying '{}' from {} to main thread",
                apiName, ThreadContext.describeCurrentThread());
        }
    }
}

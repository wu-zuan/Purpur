package org.purpurmc.purpur.threading.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method or class as thread-safe.
 * <p>
 * Methods annotated with this can be safely called from any thread
 * (main thread, region threads, async threads) without external synchronization.
 * </p>
 * <p>
 * This is a documentation annotation — it does not enforce thread safety at runtime.
 * It serves as a contract between the implementor and callers.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface ThreadSafe {
    /**
     * Optional description of why this method/class is thread-safe.
     * For example: "Uses ConcurrentHashMap internally" or "Immutable state".
     */
    String value() default "";
}

package org.purpurmc.purpur.threading.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as requiring execution on the main server thread.
 * <p>
 * When {@link #autoProxy()} is true (default), calls from non-main threads
 * will be automatically proxied back to the main thread via
 * {@link org.purpurmc.purpur.threading.MainThreadProxy}.
 * </p>
 * <p>
 * When {@link #autoProxy()} is false, calling from a non-main thread
 * will throw an {@link IllegalStateException}.
 * </p>
 * <p>
 * This is primarily a documentation annotation. Automatic proxying behavior
 * must be explicitly implemented at the call site using
 * {@link org.purpurmc.purpur.threading.ThreadSafeBukkitAPI}.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface MainThreadOnly {
    /**
     * Optional description of why this method must run on the main thread.
     * For example: "Modifies global scoreboard state" or "Accesses non-synchronized entity list".
     */
    String value() default "";

    /**
     * If true, calls from non-main threads should be automatically proxied
     * to the main thread. If false, an exception should be thrown instead.
     */
    boolean autoProxy() default true;
}

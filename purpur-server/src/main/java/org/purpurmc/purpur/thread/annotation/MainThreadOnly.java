package org.purpurmc.purpur.thread.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method, type, or field that <strong>must only</strong> be accessed from the
 * main Minecraft server thread (or a Purpur region thread during its tick window, which
 * is treated as equivalent for the annotated world's context).
 *
 * <p>Calling annotated code from any other thread risks data corruption or
 * {@link IllegalStateException}. Use
 * {@link org.purpurmc.purpur.thread.ThreadContext#ensureMainThread(Runnable)} to
 * safely defer the call to the main thread when the caller may be on a different thread.
 *
 * @see ThreadSafe
 * @see org.purpurmc.purpur.thread.TaskQueue
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.FIELD})
public @interface MainThreadOnly {

    /**
     * Optional description of why this method requires the main thread.
     */
    String value() default "Must be called from the main server thread.";
}

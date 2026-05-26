package org.purpurmc.purpur.thread.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method, type, or field as safe to call from any thread — including
 * Purpur's region threads and external async plugin threads.
 *
 * <p>This annotation is purely documentary; the runtime does not enforce thread-safety
 * automatically.  Authors are responsible for ensuring the annotated code upholds the
 * guarantee (e.g., using {@code volatile}, {@code synchronized}, or
 * {@link java.util.concurrent.atomic} primitives).
 *
 * @see MainThreadOnly
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE, ElementType.FIELD})
public @interface ThreadSafe {

    /**
     * Optional description of how thread-safety is achieved (e.g., "uses ConcurrentHashMap",
     * "immutable after construction").
     */
    String value() default "May be called from any thread.";
}
